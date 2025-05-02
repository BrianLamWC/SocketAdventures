#include <unistd.h>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <stdlib.h>

#include "batcher.h"

void Batcher::batchRequests()
{

    while (true)
    {
        batch_ = request_queue_.popAll();

        if (!batch_.empty()) {
            processBatch_();
        }

        batch_.clear();

        sleep(5);
    }
    
    pthread_exit(NULL);

}

void Batcher::processBatch_()
{
    std::vector<request::Request> batch_for_partial_sequencer;

    for (const request::Request&req_proto : batch_)
    {
        const request::Transaction& txn = req_proto.transaction(0);

        std::unordered_set<int32_t> target_peers;
        printf("BATCHER: Transaction %s for client %d:\n", txn.id().c_str(), txn.client_id());

        bool validTransaction = true;
        for (const auto& op : txn.operations())
        {
            auto it = mockDB.find(op.key());
            if (it == mockDB.end()) {
                printf("  Key %s not found in the mock database\n", op.key().c_str());
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

        for (auto serverID : target_peers)
        {
            if (serverID == my_id)
            {
                batch_for_partial_sequencer.push_back(req_proto);
            }
            else
            {
                // Send to a remote server.
                sendTransaction_(txn, serverID);
            }
        }

    }

    if (!batch_for_partial_sequencer.empty())
    {
        for (const auto& req_proto : batch_for_partial_sequencer){

            batcher_to_partial_sequencer_queue_.push(req_proto);

        }
    }
}

void Batcher::sendTransaction_(const request::Transaction& txn, const int32_t& id)
{
    std::string ip; 
    int port;

    // Find server details by id
    for (const auto& server : servers)
    {
        if (server.id == id)
        {
            ip = server.ip;
            port = server.port;
            break;
        }
    }
    
    if (ip.empty() || port == 0)
    {
        perror("Batcher::sendTransaction: ip empty or port = 0");
        return;
    }

    int connfd = setupConnection(ip, port);
    if (connfd < 0)
    {
        perror("Batcher::sendTransaction: connfd < 0");
        return;
    }
    
    // Create a Request message
    request::Request request;

    // Set the recipient, server_id, and optionally the client_id
    request.set_recipient(request::Request::PARTIAL);
    request.set_server_id(my_id);
    if (txn.has_client_id())
    {
        request.set_client_id(txn.client_id());
    }
    
    request.add_transaction()->CopyFrom(txn);
    
    // 1) serialize to string
    std::string serialized_request;
    if (!request.SerializeToString(&serialized_request)) {
        perror("SerializeToString failed");
        close(connfd);
        return;
    }

    // Print the request size
    size_t request_size = serialized_request.size();
    printf("BATCHER: Serialized request size: %zu bytes\n", request_size);

    // 2) send 4-byte length prefix (network order)
    uint32_t len = htonl(static_cast<uint32_t>(request_size));
    if (!writeNBytes(connfd, &len, sizeof(len))) {
        perror("writeNBytes (length prefix) failed");
        close(connfd);
        return;
    }

    // Optionally, print that you’re now sending the framed message
    printf("BATCHER: Sending %zu-byte request (plus 4-byte header) to %s:%d\n",
           request_size, ip.c_str(), port);

    if (!writeNBytes(connfd, serialized_request.data(), request_size)) {
        perror("writeNBytes (request) failed");
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
