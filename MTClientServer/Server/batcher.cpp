#include <unistd.h>
#include <cstring>
#include <unordered_map>
#include "batcher.h"
#include "../proto/request.pb.h"
#include <stdlib.h>

void Batcher::batchRequests()
{

    while (true)
    {
        batch = requestQueue.popAll();

        if (!batch.empty()) {
            processBatch();
        }

        batch.clear();

        sleep(5);
    }
    
    pthread_exit(NULL);

}

void Batcher::processBatch()
{

    for (const Transaction &txn : batch)
    {
        std::vector<Operation> operations = txn.getOperations();

        printf("(BATCHER) Transaction for client %s:\n", txn.getClientId().c_str());

        for (const auto& op : operations) {

            // Convert the enum type to string
            std::string operationTypeStr = (op.type == OperationType::WRITE) ? "WRITE" : "READ";
            
            // For write operations, print the value as well
            if (op.type == OperationType::WRITE) {
                printf("  Operation: %s, Key: %s, Value: %s\n", operationTypeStr.c_str(), op.key.c_str(), op.value.c_str());
            } else {
                // For read operations, value is not printed
                printf("  Operation: %s, Key: %s\n", operationTypeStr.c_str(), op.key.c_str());
            }

            // Search for the key in the mockDB
            auto it = mockDB.find(op.key);
            DataItem data_item;

            if (it != mockDB.end()) {
                // Key was found, get the DataItem
                data_item = it->second;

            } else {
                // Key was not found in the mockDB 
                printf("  Key %s not found in the mock database\n", op.key.c_str());
                break; //move to next transaction and ignore this one for now
            }
            
            if (data_item.primaryCopyID == my_id)
            {
                // push into own partial sequence
                partialSequence.push(txn);
            }else{

                sendTransaction(txn, data_item.primaryCopyID); 

            }


        }
        
    }
    
}

void Batcher::sendTransaction(const Transaction& txn, const std::string& id)
{
    // note: maybe use map?
   
    std::string ip; 
    int port;

    for (const auto& server : servers)
    {
        if (server.id == id)
        {
            ip = server.ip;
            port = server.port;
            break;
        }
        
    }
    
    if (ip.length() == 0 || port == 0)
    {
        perror("Batcher::sendTransaction: ip length or port = 0");
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

    // Set the recipient
    request.set_recipient(request::Request::PARTIAL);

    // Set server_id
    request.set_server_id(my_id);

    // Set client_id
    request.set_client_id(txn.getClientId());

    // Create transaction
    request::Transaction *transaction = request.add_transaction();

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

// Constructor
Batcher::Batcher()
{

    if (pthread_create(&batcher_thread, NULL, [](void* arg) -> void* {
            static_cast<Batcher*>(arg)->Batcher::batchRequests();
            return nullptr;
        }, this) != 0) 
    {
        threadError("Error creating batcher thread");
    }
    
    pthread_detach(batcher_thread);

}
