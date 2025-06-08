#include <chrono>
#include "partialSequencer.h"
#include <netinet/in.h>
#include <thread>


namespace {
    // compile-time constant for a 5s window
    constexpr std::chrono::milliseconds ROUND_PERIOD{500};

}

void PartialSequencer::processPartialSequence(){

    // Wait until logical clock is ready
    while (!LOGICAL_EPOCH_READY.load()) {
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
        auto next_timestamp = LOGICAL_EPOCH + std::chrono::milliseconds((current_window+1) * ROUND_PERIOD.count());

        // build the request
        partial_sequence_.Clear();
        partial_sequence_.set_server_id(my_id);
        partial_sequence_.set_recipient(request::Request::MERGER);
        partial_sequence_.set_round(static_cast<int32_t>(current_window));
        for (const auto& txn : transactions_received) {
            partial_sequence_.add_transaction()->CopyFrom(txn.transaction(0));
        }

        // print current round
        //printf("PARTIAL: in round %ld\n", current_window);

        // push & notify (even if empty!)
        partial_sequencer_to_merger_queue_.push(partial_sequence_);
        partial_sequencer_to_merger_queue_cv.notify_one();

        // print txns in the partial sequence
        // for (const auto& txn : partial_sequence_.transaction()) {
        //     printf("PARTIAL: Transaction %s for client %d:\n", txn.id().c_str(), txn.client_id());
        // }

        // send to peers (even if empty!)
        sendPartialSequence();

        transactions_received.clear();
        std::this_thread::sleep_until(next_timestamp);

    }
}


void PartialSequencer::sendPartialSequence() {
    for (auto &target : target_peers)
    {
        int target_id = target.first;
        int& connfd = merger_fds[target_id];

        partial_sequence_.set_target_server_id(target_id);

        // (re)connect on-demand if we lost it
        if (connfd < 0) {

            server target = target_peers[target_id];

            while ((connfd = setupConnection(target.ip, target.port)) < 0) {
                std::cerr << "Partial Sequencer " << my_id << ": reconnect to peer " << target_id << " failed, retrying in 1s…\n";
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            merger_fds[target_id] = connfd; // update the fd in the map
            
        }

        std::string serialized_request;
        if (!partial_sequence_.SerializeToString(&serialized_request)) {
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

void PartialSequencer::pushReceivedTransactionIntoPartialSequence(const request::Request& req_proto){
    // expect one transaction only
    batcher_to_partial_sequencer_queue_.push(req_proto);

}

PartialSequencer::PartialSequencer(){
    
    if (pthread_create(&partial_sequencer_thread, NULL, [](void* arg) -> void* {
            static_cast<PartialSequencer*>(arg)->PartialSequencer::processPartialSequence();
            return nullptr;
        }, this) != 0) 
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

