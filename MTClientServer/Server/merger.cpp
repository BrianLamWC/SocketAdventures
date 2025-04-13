#include "merger.h"
#include "utils.h"

void Merger::processProtoRequest(const request::Request& req_proto)
{
    // Check that the request contains a server_id
    if (!req_proto.has_server_id()) {
        perror("MERGER::protoRequestToPartialSequence: request has no server_id");
        return;
    }
    
    int32_t server_id = req_proto.server_id();

    // Only accept requests from expected servers.
    if (round_requests.find(server_id) != round_requests.end()) {
        round_requests[server_id]->push(req_proto);
        round_cv.notify_one();
        printf("pushed\n");
    } else {
        perror("MERGER::pushReceivedTransactionIntoPartialSequence: received request from unknown server");
        return;
    }

}

void Merger::processLocalRequests() // receives local partial sequence
{
    while (true)
    {
        std::unique_lock<std::mutex> lock(partial_sequencer_to_merger_queue_mtx);
    
        partial_sequencer_to_merger_queue_cv.wait(lock, []{
            return !partial_sequencer_to_merger_queue_.empty();
        });
    
        request::Request req_proto = partial_sequencer_to_merger_queue_.pop();
        round_requests[my_id]->push(req_proto);
        round_cv.notify_one();
        printf("pushed local\n");
    }
}

void Merger::temp()
{

    while (true)
    {
        std::unique_lock<std::mutex> lock(round_mutex);
        // Wait until each expected server has at least one request.
        round_cv.wait(lock, [this]() {
            for (auto id : expected_server_ids) {
                // If no queue exists for a server or if its queue is empty, wait.
                if (round_requests.find(id) == round_requests.end() || round_requests[id]->empty())
                    return false;
            }
            return true;
        });
        
        // At this point, we have at least one request from each expected server.
        processRoundRequests();
    
    }

}

void Merger::processRoundRequests()
{
    printf("Processing round of requests:\n");
    for (const int32_t &server_id : expected_server_ids) {
        // For each server, get the first (oldest) request.
        const request::Request& req_proto = round_requests[server_id]->pop();

        auto it = partial_sequences.find(server_id);
        auto &inner_map = it->second; // this is a unique_ptr to an unordered_map<string, Transaction>

        // create a Transaction() for each transaction in the proto request
        for (const auto &txn_proto : req_proto.transaction()){

            std::vector<Operation> operations = getOperationsFromProtoTransaction(txn_proto);
            
            Transaction transaction(txn_proto.order(), txn_proto.client_id(), operations, txn_proto.id());

            inner_map->push_back(transaction);
        }

        printf("  Server %d: %d transactions received\n", server_id, req_proto.transaction_size());
        // Here, add your merging or processing logic as needed.
    }
}

Merger::Merger()
{
    for (const auto& server : servers)
    {
        round_requests[server.id] = std::make_unique<Queue_TS<request::Request>>();
        partial_sequences[server.id] = std::make_unique<std::vector<Transaction>>();
        expected_server_ids.push_back(server.id);
    }
    
    // Create a merger thread that calls the temp() method.
    if (pthread_create(&merger_thread, nullptr, [](void* arg) -> void* {
            static_cast<Merger*>(arg)->temp();
            return nullptr;
        }, this) != 0)
    {
        threadError("Error creating merger thread");
    }
    
    pthread_detach(merger_thread);

    // Create a local popper thread that calls the processLocalRequests() method.
    if (pthread_create(&popper, nullptr, [](void* arg) -> void* {
            static_cast<Merger*>(arg)->processLocalRequests();
            return nullptr;
        }, this) != 0)
    {
        threadError("Error creating merger thread");
    }
    
    pthread_detach(popper);
}
    