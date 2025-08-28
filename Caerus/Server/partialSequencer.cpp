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
    while (!LOGICAL_EPOCH_READY.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    int64_t window = 0;
    while (true) {
        
        auto deadline = LOGICAL_EPOCH + std::chrono::milliseconds((window+1) * ROUND_PERIOD.count());

        // sleep until that moment
        std::this_thread::sleep_until(deadline);

        // grab all requests for this window (may be empty)
        auto batch = batcher_to_partial_sequencer_queue_.popAll();

        partial_sequence_.Clear();
        partial_sequence_.set_server_id(my_id);
        partial_sequence_.set_recipient(request::Request::MERGER);
        partial_sequence_.set_round(static_cast<int32_t>(window));

        for (auto &req : batch) {
            // each req.transaction(0) is a local write for your primaries
            partial_sequence_.add_transaction()->CopyFrom(req.transaction(0));
        }

        // log if partial sequence is not empty
        // if (partial_sequence_.transaction_size() > 0)
        // {
        //   std::ofstream logf("partial_sequence_log_" + std::to_string(my_id) + ".log",
        //                       std::ios::app);
        //   if (logf) {
        //     logf << "PartialSequence window=" << window
        //          << " txns=" << partial_sequence_.transaction_size() << "\n";
        //   }
        // }

        partial_sequencer_to_merger_queue_.push(partial_sequence_);
        partial_sequencer_to_merger_queue_cv.notify_one();

        //broadcast to other regions
        sendPartialSequence();

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

    // std::ofstream init_log("partial_sequence_log_" + std::to_string(my_id) + ".log", std::ios::out | std::ios::trunc);
    // std::ofstream init_recv_log("partial_sequencer_received_log_" + std::to_string(my_id) + ".log", std::ios::out | std::ios::trunc);

    if (pthread_create(&partial_sequencer_thread, NULL, [](void *arg) -> void *
                       {
            static_cast<PartialSequencer*>(arg)->PartialSequencer::processPartialSequence();
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
