#include <chrono>

#include "partialSequencer.h"
#include "utils.h"
#include <netinet/in.h>
#include <thread>

namespace {
    // compile-time constant for a 5s window
    constexpr std::chrono::seconds ROUND_PERIOD{5};

    // initialized once at program startup
    const auto ROUND_EPOCH = std::chrono::system_clock::from_time_t(0);
}

void PartialSequencer::processPartialSequence(){
    while (true)
    {
        // pull whatever we got (might be empty)
        transactions_received = batcher_to_partial_sequencer_queue_.popAll();

        // // print transction ids
        // for (const auto& req_proto : transactions_received) {
        //     const request::Transaction& txn = req_proto.transaction(0);
        //     printf("PARTIAL: Transaction %s for client %d:\n", txn.id().c_str(), txn.client_id());
        // }

        auto now = std::chrono::system_clock::now();
        auto since_0 = now - ROUND_EPOCH;
        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(since_0).count();

        int64_t current_window = elapsed_seconds / ROUND_PERIOD.count();
        auto next_timestamp = ROUND_EPOCH + std::chrono::seconds((current_window+1) * ROUND_PERIOD.count());

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
        for (const auto& server : servers) {
            if (server.id != my_id) {
                sendPartialSequence(server.ip, server.port);
            }
        }

        transactions_received.clear();
        std::this_thread::sleep_until(next_timestamp);

    }
}


void PartialSequencer::sendPartialSequence(const std::string& ip, const int& port) {
    int connfd = setupConnection(ip, port);
    if (connfd < 0) {
        perror("connect failed");
        return;
    }

    // 1) serialize to string
    std::string request;
    if (!partial_sequence_.SerializeToString(&request)) {
        perror("SerializeToString failed");
        close(connfd);
        return;
    }

    // Print the request size
    size_t request_size = request.size();
    //printf("PARTIAL: Serialized request size: %zu bytes\n", request_size);

    // 2) send 4-byte length prefix (network order)
    uint32_t len = htonl(static_cast<uint32_t>(request_size));
    if (!writeNBytes(connfd, &len, sizeof(len))) {
        perror("writeNBytes (length prefix) failed");
        close(connfd);
        return;
    }

    // Optionally, print that you’re now sending the framed message
    // printf("PARTIAL: Sending %zu-byte request (plus 4-byte header) to %s:%d\n",
    //        request_size, ip.c_str(), port);

    if (!writeNBytes(connfd, request.data(), request_size)) {
        perror("writeNBytes (request) failed");
    }

    close(connfd);
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

}