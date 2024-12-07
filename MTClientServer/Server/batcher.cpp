#include "batcher.h"
#include <unistd.h>
#include <cstring>
#include <unordered_map>
#include "../proto/request.pb.h"

void Batcher::batchRequests()
{

    while (true)
    {
        batch = requestQueue.popAll();

        if (!batch.empty()) {
            processBatch(batch);
        }

        batch.clear();

        sleep(5);
    }
    
    pthread_exit(NULL);

}

void Batcher::processBatch(const std::vector<Transaction>& batch)
{

    for (const Transaction &txn : batch)
    {
        std::vector<Operation> operations = txn.getOperations();

        printf("(BATCHER) Transaction for client %d:\n", txn.getClientId());

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
    
    if (ip.length() == 0 && port == 0)
    {
        return;
    }

    int connfd = setupConnection(ip, port);

    if (connfd < 0)
    {
        return;
    }
    
    // create a Request message
    request::Request request;
    request.set_server_id(atoi(my_id.c_str()));

    // Set the recipient
    request.set_recipient(request::Request::PARTIAL);

    // Create transaction
    request::Transaction *transaction = request.mutable_transaction();

    std::vector<Operation> operations = txn.getOperations();

    for (const auto& operation: operations)
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
        error("error serializing request");
    }

    // Send serialized request
    int sent_bytes = write(connfd, serialized_request.c_str(), serialized_request.size());
    if (sent_bytes < 0)
    {
        error("error writing to socket");
    }

    // Close the connection
    close(connfd);

}

// Constructor
Batcher::Batcher()
{

    if (pthread_create(&batcher_thread, NULL, [](void* arg) -> void* {
            static_cast<Batcher*>(arg)->batchRequests();
            return nullptr;
        }, this) != 0) 
    {
        threadError("Error creating batcher thread");
    }
    
    pthread_detach(batcher_thread);

}
