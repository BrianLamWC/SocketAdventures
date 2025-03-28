#include "partialSequencer.h"
#include "utils.h"

void PartialSequencer::processPartialSequence(){

    while (true)
    {

        partial_sequence = batcher_to_partial_sequencer_queue.popAll();

        if (!partial_sequence.empty())
        {
            for (const auto& server : servers) 
            {

                if (server.id != my_id)
                {
                    PartialSequencer::sendPartialSequence(server.ip, server.port);
                }
                
            }
                    
        }
            
        partial_sequence.clear();
        
        sleep(5);
    }
    

}

void PartialSequencer::sendPartialSequence(const std::string& ip, const int& port){

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
    for (const auto& txn : partial_sequence)
    {
        // Create transaction
        request::Transaction *transaction = request.add_transaction();
        transaction->set_id(txn.getId());
        transaction->set_client_id(txn.getClientId());

        std::vector<Operation> operations = txn.getOperations();

        for (const auto& operation:operations)
        {
            request::Operation *op = transaction->add_operations();
        
            if ( operation.type == OperationType::WRITE )
            {
                op->set_type(request::Operation::WRITE);
                op->set_value(operation.value);
            }else{
                op->set_type(request::Operation::READ);
            }

            op->set_key(operation.key);  

        }
    
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

void PartialSequencer::pushReceivedTransactionIntoPartialSequence(const request::Request& req_proto){

    // expect one transaction only
    std::vector<Operation> operations = getOperationsFromProtoTransaction(req_proto.transaction(0));

    Transaction transaction(req_proto.transaction(0).id().c_str(), req_proto.client_id(), operations);

    batcher_to_partial_sequencer_queue.push(transaction);

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