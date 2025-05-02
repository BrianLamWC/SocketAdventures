#include "partialSequencer.h"
#include "utils.h"
#include <netinet/in.h>

void PartialSequencer::processPartialSequence(){

    while (true)
    {

        transactions_received = batcher_to_partial_sequencer_queue_.popAll();
        partial_sequence_.set_server_id(my_id);
        partial_sequence_.set_recipient(request::Request::MERGER);

        for (const auto& txn : transactions_received)
        {
            partial_sequence_.add_transaction()->CopyFrom(txn.transaction(0));
        }
        

        if (!transactions_received.empty())
        {

            partial_sequencer_to_merger_queue_.push(partial_sequence_);

            partial_sequencer_to_merger_queue_cv.notify_one();

            for (const auto& server : servers) 
            {

                if (server.id != my_id)
                {
                    PartialSequencer::sendPartialSequence(server.ip, server.port);
                }
                
            }

        }
            
        transactions_received.clear();
        partial_sequence_.Clear();
        
        sleep(5);
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
    printf("PARTIAL: Serialized request size: %zu bytes\n", request_size);

    // 2) send 4-byte length prefix (network order)
    uint32_t len = htonl(static_cast<uint32_t>(request_size));
    if (!writeNBytes(connfd, &len, sizeof(len))) {
        perror("writeNBytes (length prefix) failed");
        close(connfd);
        return;
    }

    // Optionally, print that you’re now sending the framed message
    printf("PARTIAL: Sending %zu-byte request (plus 4-byte header) to %s:%d\n",
           request_size, ip.c_str(), port);

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