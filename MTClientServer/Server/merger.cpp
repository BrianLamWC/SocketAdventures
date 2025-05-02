#include "merger.h"
#include "utils.h"

void Merger::processProtoRequest(const request::Request& req_proto)
{
    if (!req_proto.has_server_id()) {
        perror("MERGER: request has no server_id");
        return;
    }
    int32_t server_id = req_proto.server_id();

    // lock round_mutex while we push into round_requests
    {
      std::lock_guard<std::mutex> lock (round_mutex);
      auto it = round_requests.find(server_id);
      if (it == round_requests.end()) {
          fprintf(stderr,
                  "MERGER::processProtoRequest: unknown server %d\n",
                  server_id);
          return;
      }
      it->second->push(req_proto);
    } // lock goes out of scope and unlocks here

    round_cv.notify_one();
}

void Merger::processLocalRequests()
{
    while (true)
    {
        // wait on the local queue’s CV, then pop 
        request::Request req_proto;
        {
          std::unique_lock<std::mutex> local_lock(partial_sequencer_to_merger_queue_mtx);
          partial_sequencer_to_merger_queue_cv.wait(local_lock, [] {
              return !partial_sequencer_to_merger_queue_.empty();
          });
          req_proto = partial_sequencer_to_merger_queue_.pop();
        } // local_lock unlocked here

        {
          std::lock_guard<std::mutex> lk(round_mutex);
          round_requests[my_id]->push(req_proto);
        }

        round_cv.notify_one();
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
    
        {
            std::lock_guard<std::mutex> round_ready_lock(insert_mutex);
            round_ready = true;
        }

        insert_cv.notify_one();

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

            inner_map->push(transaction);
        }

        printf("  Server %d: %ld transactions queued\n", server_id, inner_map->size());
        // Here, add your merging or processing logic as needed.
    }
}

void Merger::insertAlgorithm(){

    std::unique_lock<std::mutex> lk(insert_mutex);
    while (true)
    {
        // 1) wait until temp() signals a new round
        insert_cv.wait(lk, [this](){ return round_ready; });

        // 2) reset the flag before you process
        round_ready = false;

        for (const auto &server : servers)
        {
            printf("INSERT::Server %d\n", server.id);
    
            auto it = partial_sequences.find(server.id);
            auto &inner_map = it->second;
            auto transactions = inner_map->popAll();
    
            std::unordered_set<DataItem> primary_set;
            std::unordered_map<DataItem, Transaction*> most_recent_writers; 
            std::unordered_map<DataItem, std::unordered_set<std::string>> most_recent_readers;
    
            // setup the primary set for current server
            for (const auto &txn : transactions)
            {
                for (const auto &op : txn.getOperations())
                {
                    auto db_it = mockDB.find(op.key);
    
                    if (db_it == mockDB.end()) 
                    {
                        std::cout << "INSERT::PrimarySet: key " << op.key <<" not found" << std::endl;
                        continue;
                    }
    
                    auto data_item = db_it->second;
    
                    if ( data_item.primaryCopyID == server.id)
                    {
                        primary_set.insert(data_item);
                    }
    
                    // ensure the key exists in the MRW map (value initialized to nullptr)
                    most_recent_writers.emplace(data_item, nullptr);
    
                }
            }
    
            for (const auto &txn : transactions)
            {
    
                if (graph.getNode(txn.getUUID()) == nullptr) {
                    graph.addNode(std::make_unique<Transaction>(txn));
                }
    
                std::unordered_set<DataItem> write_set; 
                std::unordered_set<DataItem> read_set;
    
                // setup the read and write set for the current transaction
                for (const auto &op : txn.getOperations())
                {
    
                    auto it = mockDB.find(op.key);
    
                    if (it == mockDB.end())
                    {
                        std::cout << "INSERT::ReadWriteSet: key " << op.key << " not found" << std::endl;
                        continue;
                    }
    
                    DataItem data_item = it->second;
    
                    if (op.type == OperationType::WRITE)
                    {
                        write_set.insert(data_item); // dont use nullptr, need to be valid pointer or just ""
                    }else if (op.type == OperationType::READ){
                        read_set.insert(data_item);
                    }
    
                    //pritn read and write set
                    std::cout << "INSERT::ReadWriteSet: key " << op.key << " type " << (op.type == OperationType::READ ? "READ" : "WRITE") << std::endl;
                
                }
                
                // Read Set ∩ Primary Set
                for (const auto &data_item : primary_set) {
                    if (read_set.find(data_item) != read_set.end()) {
    
                        std::cout << "INSERT::READSET:" << data_item.val << " is in read and primary set" << std::endl;
    
                        auto mrw_it = most_recent_writers.find(data_item); //get mrw for data item
    
                        if (mrw_it == most_recent_writers.end()) { // data item not found 
                            std::cout << "INSERT::READSET: key " << data_item.val << " not found" << std::endl;
                            continue;
                        }
    
                        //auto &mrw_txn = mrw_it->second;
    
                        if (mrw_it->second != nullptr) { // data item has mrw
                            // print transaction id
                            std::cout << "INSERT::READSET: key " << data_item.val << " has mrw " << mrw_it->second->getUUID() << std::endl;
                            
                            if (graph.getNode(mrw_it->second->getUUID()) != nullptr) { // if mrw in graph
                                std::cout << "INSERT::READSET:" << mrw_it->second->getUUID() << " in graph" << std::endl;
                                auto current_txn = graph.getNode(txn.getUUID());
                                current_txn->addNeighbor(mrw_it->second);
                                std::cout << "INSERT::READSET: adding edge from " << txn.getUUID() << " to " << mrw_it->second->getUUID() << std::endl;
                            }
                            
                        }else {
                            std::cout << "INSERT::READSET: key " << data_item.val << " has no mrw" << std::endl;
                        }
    
                        most_recent_readers[data_item].emplace(txn.getUUID()); // add current transaction to readers
    
                    }
                }
                
                // Write Set ∩ Primary Set
                for (const auto &data_item : primary_set) {
                    if (write_set.find(data_item) != write_set.end()) {
                        
                        std::cout << "INSERT::WRITESET:" <<data_item.val << " is in write and primary set" << std::endl;
    
                        auto mrw_it = most_recent_writers.find(data_item); //get mrw for data item
    
                        if (mrw_it == most_recent_writers.end()) { // data item not found 
                            std::cout << "INSERT::WRITESET: key " << data_item.val << " not found" << std::endl;
                            continue;
                        }
    
                        if (mrw_it->second != nullptr) { // data item has mrw , check if mrw is in graph
    
                            // print transaction id
                            std::cout << "INSERT::WRITESET: key " << data_item.val << " has mrw " << mrw_it->second->getUUID() << std::endl;
                            
                            if (graph.getNode(mrw_it->second->getUUID()) != nullptr) { // if mrw in graph
                                std::cout << "INSERT::WRITESET:" << mrw_it->second->getUUID() << " in graph" << std::endl;
                                auto current_txn = graph.getNode(txn.getUUID());
                                current_txn->addNeighbor(mrw_it->second);
                                std::cout << "INSERT::WRITESET: adding edge from " << txn.getUUID() << " to " << mrw_it->second->getUUID() << std::endl;
                            }
    
                        }else {
                            std::cout << "INSERT::WRITESET: key " << data_item.val << " has no mrw" << std::endl;
                        }
    
                        mrw_it->second = graph.getNode(txn.getUUID()); // set mrw to current transaction
                        
                        // readers ∩ Primary Set 
                        auto readers_it = most_recent_readers.find(data_item);
    
                        if (readers_it != most_recent_readers.end())
                        {
        
                            std::cout << "INSERT::READERS: key " << data_item.val << " has readers" << std::endl;
        
                            auto readers = readers_it->second;
        
                            for (const auto &reader_id : readers)
                            {
                                
                                std::cout << "INSERT::READERS: key " << data_item.val << " has reader " << reader_id << std::endl;
        
                                auto read_txn = graph.getNode(reader_id);
        
                                if (read_txn != nullptr) // if reader in graph
                                {
                                    auto current_txn = graph.getNode(txn.getUUID());
                                    current_txn->addNeighbor(read_txn); // add reader to current transaction
                                    std::cout << "INSERT::READERS: adding edge from " << txn.getUUID() << " to " << read_txn->getUUID() << std::endl;
                                }
                                
                            }
                            
                        }
    
                    }
    
    
                }
    
    
            }
    
    
        }
    
        graph.printAll();
        graph.clear();
    }
    
}

Merger::Merger()
{

    round_requests .rehash(1);
    partial_sequences.rehash(1);

    for (const auto& server : servers)
    {
        round_requests[server.id] = std::make_unique<Queue_TS<request::Request>>();
        partial_sequences[server.id] = std::make_unique<Queue_TS<Transaction>>();
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

    // Create an insert thread that calls the insertAlgorithm() method.
    if (pthread_create(&insert_thread, nullptr, [](void* arg) -> void* {
            static_cast<Merger*>(arg)->insertAlgorithm();
            return nullptr;
        }, this) != 0)
    {
        threadError("Error creating insert thread");
    }

    pthread_detach(insert_thread);

}
    