#include <chrono>
#include "partialSequencer.h"
#include <netinet/in.h>
#include <thread>
#include <fstream>

namespace
{
    // compile-time constant for a 5s window
    constexpr std::chrono::milliseconds ROUND_PERIOD{100};

}

void PartialSequencer::processPartialSequence()
{

    // Wait until logical clock is ready
    while (!LOGICAL_EPOCH_READY.load())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    while (true)
    {
        // pull whatever we got (might be empty)
        transactions_received = batcher_to_partial_sequencer_queue_.popAll();

        auto now = std::chrono::steady_clock::now();
        auto since_0 = now - LOGICAL_EPOCH;
        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::milliseconds>(since_0).count();

        int64_t current_window = elapsed_seconds / ROUND_PERIOD.count();
        auto next_timestamp = LOGICAL_EPOCH + std::chrono::milliseconds((current_window + 1) * ROUND_PERIOD.count());

        // build the request
        partial_sequence_.Clear();
        partial_sequence_.set_server_id(my_id);
        partial_sequence_.set_recipient(request::Request::MERGER);
        partial_sequence_.set_round(static_cast<int32_t>(current_window));
        for (const auto &txn : transactions_received)
        {
            partial_sequence_.add_transaction()->CopyFrom(txn.transaction(0));
        }

        // push & notify (even if empty!)
        partial_sequencer_to_merger_queue_.push(partial_sequence_);
        partial_sequencer_to_merger_queue_cv.notify_one();

        // send to peers (even if empty!)
        sendPartialSequence();

        transactions_received.clear();
        std::this_thread::sleep_until(next_timestamp);
    }
}

// in partialSequencer.cpp:
void PartialSequencer::processPartialSequence2()
{
    // wait for the logical epoch
    while (!LOGICAL_EPOCH_READY.load())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // the next batch_round we want to output
    int32_t next_output_round_ = 0;  // or 1, whichever matches your Batcher

    // buffer: batcher_round -> list of Request protos
    std::map<int32_t, std::vector<request::Request>> pending;

    while (true)
    {
        // block until there’s at least one txn to send
        while (batcher_to_partial_sequencer_queue_.empty())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // pull them all out
        auto transactions_received = batcher_to_partial_sequencer_queue_.popAll();

        // Log the received transaction
        std::ofstream log_file("partial_sequencer_received_log_" + std::to_string(my_id) + ".log", std::ios::app);
        if (log_file)
        {
            log_file << "Transaction Received by Partial Sequencer\n";
            log_file << "  Transactions:\n";

            for (const auto &req :transactions_received)
            {
                auto &txn = req.transaction(0);

                log_file << "    Server ID: " << req.server_id() << "\n";
                log_file << "    Transaction ID: " << txn.id() << "\n";
                log_file << "    batcher_round: " << req.batcher_round() << "\n";
                log_file << "    Operations:\n";
                for (const auto &op : txn.operations())
                {
                    log_file << "      - Type: " << (op.type() == request::Operation::WRITE ? "WRITE" : "READ")
                            << ", Key: " << op.key();
                    if (op.type() == request::Operation::WRITE)
                    {
                        log_file << ", Value: " << op.value();
                    }
                    log_file << "\n";
                }
            }
            log_file << "----------------------------------------\n";
        }
        else
        {
            std::cerr << "Failed to open log file for partial sequencer " << my_id << "\n";
        }


        for (auto &req_proto : transactions_received) {
            if (req_proto.has_batcher_round()) {
                auto br = req_proto.batcher_round();
                pending[br].push_back(std::move(req_proto));
            } else {
                continue;
            }
        }        

        while (true)
        {
            // if we have no pending requests for the next output round, break
            auto it = pending.find(next_output_round_);
            if (it == pending.end()) break;

            // build one partial sequence for this round
            partial_sequence_.Clear();
            partial_sequence_.set_server_id(my_id);
            partial_sequence_.set_recipient(request::Request::MERGER);
            partial_sequence_.set_round(next_output_round_);

            for (const auto &txn : it->second)
            {
                partial_sequence_.add_transaction()->CopyFrom(txn.transaction(0));
            }        
            
            // Log the partial sequence
            std::ofstream log_file("partial_sequence_log_" + std::to_string(my_id) + ".log", std::ios::app);
            if (log_file)
            {
                log_file << "Partial Sequence Created:\n";
                log_file << "  Server ID: " << partial_sequence_.server_id() << "\n";
                log_file << "  Round: " << partial_sequence_.round() << "\n";
                log_file << "  Transactions:\n";
                for (const auto &txn : partial_sequence_.transaction())
                {
                    log_file << "    Transaction ID: " << txn.id() << "\n";
                    log_file << "    Operations:\n";
                    for (const auto &op : txn.operations())
                    {
                        log_file << "      - Type: " << (op.type() == request::Operation::WRITE ? "WRITE" : "READ")
                                << ", Key: " << op.key();
                        if (op.type() == request::Operation::WRITE)
                        {
                            log_file << ", Value: " << op.value();
                        }
                        log_file << "\n";
                    }
                }
                log_file << "----------------------------------------\n";
            }
            else
            {
                std::cerr << "Failed to open log file for partial sequencer " << my_id << "\n";
            }

            // PUSH & NOTIFY downstream
            partial_sequencer_to_merger_queue_.push(partial_sequence_);
            partial_sequencer_to_merger_queue_cv.notify_one();

            // SEND to peers
            sendPartialSequence();

            // remove and advance
            pending.erase(it);
            next_output_round_++;
        }

    }
}

void PartialSequencer::processPartialSequence3()
{
    // 1) wait until logical epoch is ready
    while (!LOGICAL_EPOCH_READY.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    int64_t window = 0;
    while (true) {
        // 2) compute when window “window” ends
        auto deadline = LOGICAL_EPOCH
                      + std::chrono::milliseconds((window+1) * ROUND_PERIOD.count());

        // 3) sleep until that moment
        std::this_thread::sleep_until(deadline);

        // 4) grab *all* Requests for this window (may be empty)
        auto batch = batcher_to_partial_sequencer_queue_.popAll();

        // 5) build the partial_sequence
        partial_sequence_.Clear();
        partial_sequence_.set_server_id(my_id);
        partial_sequence_.set_recipient(request::Request::MERGER);
        partial_sequence_.set_round(static_cast<int32_t>(window));

        for (auto &req : batch) {
            // each req.transaction(0) is a local write for your primaries
            partial_sequence_.add_transaction()->CopyFrom(req.transaction(0));
        }

        // // 6) log if partial sequence is not empty
        // if (partial_sequence_.transaction_size() > 0)
        // {
        //   std::ofstream logf("partial_sequence_log_" + std::to_string(my_id) + ".log",
        //                       std::ios::app);
        //   if (logf) {
        //     logf << "PartialSequence window=" << window
        //          << " txns=" << partial_sequence_.transaction_size() << "\n";
        //   }
        // }

        // 7) hand off to the Merger
        partial_sequencer_to_merger_queue_.push(partial_sequence_);
        partial_sequencer_to_merger_queue_cv.notify_one();

        // 8) broadcast to other regions
        sendPartialSequence();

        // 9) advance to next window
        window++;
    }
}

void PartialSequencer::sendPartialSequence()
{
    for (auto &target : target_peers)
    {
        int target_id = target.first;
        int &connfd = merger_fds[target_id];

        partial_sequence_.set_target_server_id(target_id);

        // (re)connect on-demand if we lost it
        if (connfd < 0)
        {

            server target = target_peers[target_id];

            while ((connfd = setupConnection(target.ip, target.port)) < 0)
            {
                std::cerr << "Partial Sequencer " << my_id << ": reconnect to peer " << target_id << " failed, retrying in 1s…\n";
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            merger_fds[target_id] = connfd; // update the fd in the map
        }

        std::string serialized_request;
        if (!partial_sequence_.SerializeToString(&serialized_request))
        {
            perror("SerializeToString failed");
            close(connfd);
            return;
        }

        uint32_t netlen = htonl(uint32_t(serialized_request.size()));
        if (!writeNBytes(connfd, &netlen, sizeof(netlen)) ||
            !writeNBytes(connfd, serialized_request.data(), serialized_request.size()))
        {
            perror("writeNBytes failed");
            // connection broken, force reconnect
            close(connfd);
            connfd = -1;
        }
    }
}

void PartialSequencer::pushReceivedTransactionIntoPartialSequence(const request::Request &req_proto)
{
    // Push the transaction into the queue
    batcher_to_partial_sequencer_queue_.push(req_proto);
}

PartialSequencer::PartialSequencer()
{

    std::ofstream init_log("partial_sequence_log_" + std::to_string(my_id) + ".log", std::ios::out | std::ios::trunc);
    std::ofstream init_recv_log("partial_sequencer_received_log_" + std::to_string(my_id) + ".log", std::ios::out | std::ios::trunc);

    if (pthread_create(&partial_sequencer_thread, NULL, [](void *arg) -> void *
                       {
            static_cast<PartialSequencer*>(arg)->PartialSequencer::processPartialSequence3();
            return nullptr; }, this) != 0)
    {
        threadError("Error creating batcher thread");
    }

    pthread_detach(partial_sequencer_thread);

    for (auto &server : servers)
    {
        if (server.id != my_id)
        {
            target_peers[server.id] = server;
            merger_fds[server.id] = -1;
        }
    }
}
