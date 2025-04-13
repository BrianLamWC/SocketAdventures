#include "partialSequencer.h"
#include "utils.h"

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
                    PartialSequencer::sendPartialSequence_(server.ip, server.port);
                }
                
            }

        }
            
        transactions_received.clear();
        partial_sequence_.Clear();
        
        sleep(5);
    }
    

}

void PartialSequencer::sendPartialSequence_(const std::string& ip, const int& port){

    if (ip.length() == 0 || port == 0)
    {
        perror("PartialSequencer::sendPartialSequence: ip length or port = 0");
        return;
    }

    int connfd = setupConnection(ip, port);

    if (connfd < 0)
    {
        perror("PartialSequencer::sendTransaction: connfd < 0");
        return;
    }
    
    // create a Request message
    request::Request request;
    request.set_server_id(my_id);

    // set the recipient
    request.set_recipient(request::Request::MERGER);    

    // add transactions 
    for (const auto& txn : transactions_received)
    {
        // copy transactions into request
        request.add_transaction()->CopyFrom(txn.transaction(0));
    
    }
    
    // Serialize the Request message
    std::string serialized_request;
    if (!request.SerializeToString(&serialized_request))
    {
        perror("error serializing request");
        return;
    }

    // Send serialized request
    int sent_bytes = write(connfd, serialized_request.c_str(), serialized_request.size());
    if (sent_bytes < 0)
    {
        perror("error writing to socket");
        return;
    }

    // Close the connection
    close(connfd);
    
}

void PartialSequencer::pushReceivedTransactionIntoPartialSequence_(const request::Request& req_proto){

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