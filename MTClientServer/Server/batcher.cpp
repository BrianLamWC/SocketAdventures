#include <unistd.h>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <stdlib.h>

#include "batcher.h"
#include "../proto/request.pb.h"


void Batcher::batchRequests()
{

    while (true)
    {
        batch = request_queue.popAll();

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
    std::vector<Transaction> batch_for_partial_sequencer;

    for (const Transaction &txn : batch)
    {
        std::vector<Operation> operations = txn.getOperations();
        std::unordered_set<int32_t> target_peers;

        printf("BATCHER: Transaction %s for client %d:\n", txn.getId(), txn.getClientId());

        bool validTransaction = true;
        for (const auto& op : operations)
        {
            auto it = mockDB.find(op.key);
            if (it == mockDB.end()) {
                printf("  Key %s not found in the mock database\n", op.key.c_str());
                validTransaction = false;
                break;
            }
            
            const DataItem& data_item = it->second;
            // Add the primary copy ID to the set.
            target_peers.insert(data_item.primaryCopyID);
        }
        
        if (!validTransaction){
            continue;
        }

        // Now send the transaction once per unique target
        for (auto serverID : target_peers)
        {
            if (serverID == my_id)
            {
                batch_for_partial_sequencer.push_back(txn);
            }
            else
            {
                // Send to a remote server.
                sendTransaction(txn, serverID);
            }
        }

    }

    if (!batch_for_partial_sequencer.empty())
    {
        for (const auto& txn : batch_for_partial_sequencer){

            batcher_to_partial_sequencer_queue.push(txn);

        }
    }
    
}

void Batcher::sendTransaction(const Transaction& txn, const int32_t& id)
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
    transaction->set_id(txn.getId());

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
