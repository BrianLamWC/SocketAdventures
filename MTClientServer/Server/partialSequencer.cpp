#include "partialSequencer.h"
#include "utils.h"

void PartialSequencer::processPartialSequence(){

    while (true)
    {

        my_partial_sequence = partialSequence.popAll();

        if (!my_partial_sequence.empty())
        {
            
            for (const auto& server : servers) 
            {

                if (server.id != my_id)
                {
                    PartialSequencer::sendPartialSequence(server.ip, server.port);
                }
                
            }
                    
        }
        
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
        perror("Batcher::sendTransaction: connfd < 0");
        return;
    }
    
    // create a Request message
    request::Request request;
    request.set_server_id(my_id);

    // Set the recipient
    request.set_recipient(request::Request::MERGER);    

    for (const auto& transaction : my_partial_sequence)
    {
        // Create transaction
        request::Transaction *txn = request.add_transaction();
        std::vector<Operation> operations = transaction.getOperations();

        for (const auto& operation:operations)
        {
            request::Operation *op = txn->add_operations();
        
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

    Transaction transaction(req_proto.client_id(), operations);

    partialSequence.push(transaction);

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