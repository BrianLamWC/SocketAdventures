#include "merger.h"
#include "utils.h"

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

            processIncomingRequest3(req_proto);
        } 

    }
}

void Merger::processRoundRequests()
{
    //printf("Processing round of requests:\n");
    for (const auto &txn : current_batch)
    {
        auto it = partial_sequences.find(txn.server_id());
        auto &inner_map = it->second; // this is a unique_ptr to an unordered_map<string, Transaction>

        for (const auto &txn_proto : txn.transaction())
        {

            std::vector<Operation> operations = getOperationsFromProtoTransaction(txn_proto);
            
            Transaction transaction(txn_proto.order(), txn_proto.client_id(), operations, txn_proto.id());

            inner_map->push(transaction);

        }
        //printf("  Server %d: %ld transactions queued\n", txn.server_id(), inner_map->size());
    }

}

void Merger::processIncomingRequest(const request::Request& req_proto) {
    if (!req_proto.has_server_id() || !req_proto.has_round()) { return; }
    int32_t sid = req_proto.server_id();
    int32_t rnd = req_proto.round();

    //printf("MERGER: Received request from server %d for round %d\n", sid, rnd);

    std::lock_guard<std::mutex> lk(round_mutex);
    auto &bucket = pending_rounds[rnd];
    bucket[sid] = req_proto;  // overwrite if we already had one

    // if we now have one from *every* server, we can proceed
    if (bucket.size() == expected_server_ids.size()) {
        // collect in server‐order
        current_batch.clear();
        current_batch.reserve(expected_server_ids.size());
        for (auto id : expected_server_ids) {
            current_batch.push_back(std::move(bucket[id]));
        }
        // forget this round
        pending_rounds.erase(rnd);

        processRoundRequests();

        // signal the insert thread
        {
          std::lock_guard<std::mutex> g(insert_mutex);
          round_ready = true;
        }

        insert_cv.notify_one();
    }
}

void Merger::processIncomingRequest2(const request::Request& req_proto){

    if (!req_proto.has_server_id() || !req_proto.has_round()) { return; }
    int32_t sid = req_proto.server_id();
    int32_t rnd = req_proto.round();

    std::lock_guard<std::mutex> lk(round_mutex);
    auto &bucket = pending_rounds[rnd];
    bucket[sid] = req_proto;


    if (bucket.size() == expected_server_ids.size())
    {
        std::vector<request::Request> batch;
        batch.reserve(bucket.size());
        
        for (const auto &id : expected_server_ids)
        {
            batch.push_back(std::move(bucket[id]));
        }
        
        pending_rounds.erase(rnd);
        ready_rounds.emplace(rnd, std::move(batch));

    }
    
    while (!ready_rounds.empty())
    {
        auto it = ready_rounds.begin();  // smallest round
        
        if(it->first <= last_round) {
            // stale round
            ready_rounds.erase(it);
            continue;
        }

        last_round = it->first;

        current_batch.clear();
        current_batch = std::move(it->second);

        ready_rounds.erase(it);

        processRoundRequests();
        
        {
          std::lock_guard<std::mutex> lock(insert_mutex);
          round_ready = true;
        }

        insert_cv.notify_one();

    }
    

}

void Merger::processIncomingRequest3(const request::Request& req_proto){

    if (!req_proto.has_server_id() || !req_proto.has_round()) return;

    int32_t sid = req_proto.server_id();

    // 1) push into that server's heap
    {
      std::lock_guard<std::mutex> lk(round_mutex);
      pending_heaps[sid].push(req_proto);

      // 2) check if every server has at least one pending request
      for (auto id : expected_server_ids) {
        if (pending_heaps[id].empty()) {
          return;  // still waiting on some server
        }
      }

      // 3) all servers ready → build the batch
      current_batch.clear();
      current_batch.reserve(expected_server_ids.size());
      for (auto id : expected_server_ids) {
        auto &heap = pending_heaps[id];
        const auto &top_req = heap.top();

        // Only log if this request has transactions
        if (top_req.transaction_size() > 0) {
          std::ostringstream oss;
          oss << "Server " << id
              << " (round " << top_req.round() << ") txns: ";
          for (const auto &txn_proto : top_req.transaction()) {
            oss << txn_proto.id() << " ";
          }
          auto s = oss.str();
          // trim trailing space
          if (!s.empty() && s.back()==' ') s.pop_back();
          std::ofstream log_file("./logs/merger_log" + std::to_string(my_id) + ".txt", std::ios::app);
          if (log_file.is_open()) {
            log_file << s << std::endl;
          }
        }
        // now consume it
        current_batch.push_back(std::move(const_cast<request::Request&>(top_req)));
        heap.pop();
      }
    } // unlock round_mutex

    // 4) now you have one lowest-round request from each server
    processRoundRequests();

    // 5) signal the inserter
    {
      std::lock_guard<std::mutex> g(insert_mutex);
      round_ready = true;
    }
    insert_cv.notify_one();

}

void Merger::insertAlgorithm(){

    std::unique_lock<std::mutex> lock(insert_mutex);
    while (true)
    {
        insert_cv.wait(lock, [this](){ return round_ready; });
        round_ready = false;

        for (const auto &server : servers)
        {
            //printf("INSERT::Server %d\n", server.id);
    
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
    
            for (auto &txn : transactions)
            {
                //std::cout << "INSERT::Transaction: " << txn.getUUID() << std::endl;
    
                std::unordered_set<DataItem> write_set; 
                std::unordered_set<DataItem> read_set;
                std::unordered_set<int32_t> expected_regions;
    
                // setup the read and write set for the current transaction
                for (const auto &op : txn.getOperations())
                {
    
                    auto it = mockDB.find(op.key);
    
                    if (it == mockDB.end())
                    {
                        //std::cout << "INSERT::ReadWriteSet: key " << op.key << " not found" << std::endl;
                        continue;
                    }
    
                    DataItem data_item = it->second;
    
                    if (op.type == OperationType::WRITE)
                    {
                        write_set.insert(data_item); // dont use nullptr, need to be valid pointer or just ""
                    }else if (op.type == OperationType::READ){
                        read_set.insert(data_item);
                    }
    
                    expected_regions.insert(data_item.primaryCopyID); // add primary copy id to expected regions

                    //pritn read and write set
                    //std::cout << "INSERT::ReadWriteSet: key " << op.key << " type " << (op.type == OperationType::READ ? "READ" : "WRITE") << std::endl;
                
                }
                
                auto curr_txn = graph.getNode(txn.getUUID());

                if (curr_txn == nullptr) {
                    txn.setExpectedRegions(expected_regions);
                    txn.addSeenRegion(server.id);
                    graph.addNode(std::make_unique<Transaction>(txn));
                }else{
                    curr_txn->addSeenRegion(server.id);
                }

                // Read Set ∩ Primary Set
                for (const auto &data_item : primary_set) {
                    if (read_set.find(data_item) != read_set.end()) {
    
                        //std::cout << "INSERT::READSET:" << data_item.val << " is in read and primary set" << std::endl;
    
                        auto mrw_it = most_recent_writers.find(data_item); //get mrw for data item
    
                        if (mrw_it == most_recent_writers.end()) { // data item not found 
                            //std::cout << "INSERT::READSET: key " << data_item.val << " not found" << std::endl;
                            continue;
                        }
    
                        //auto &mrw_txn = mrw_it->second;
    
                        if (mrw_it->second != nullptr) { // data item has mrw
                            // print transaction id
                            //std::cout << "INSERT::READSET: key " << data_item.val << " has mrw " << mrw_it->second->getUUID() << std::endl;
                            
                            if (graph.getNode(mrw_it->second->getUUID()) != nullptr) { // if mrw in graph
                                //std::cout << "INSERT::READSET:" << mrw_it->second->getUUID() << " in graph" << std::endl;
                                auto current_txn = graph.getNode(txn.getUUID());
                                current_txn->addNeighborOut(mrw_it->second);
                                //std::cout << "INSERT::READSET: adding edge from " << txn.getUUID() << " to " << mrw_it->second->getUUID() << std::endl;
                            }
                            
                        }else {
                            //std::cout << "INSERT::READSET: key " << data_item.val << " has no mrw" << std::endl;
                        }
    
                        most_recent_readers[data_item].emplace(txn.getUUID()); // add current transaction to readers
    
                    }
                }
                
                // Write Set ∩ Primary Set
                for (const auto &data_item : primary_set) {
                    if (write_set.find(data_item) != write_set.end()) {
                        
                        //std::cout << "INSERT::WRITESET:" <<data_item.val << " is in write and primary set" << std::endl;
    
                        auto mrw_it = most_recent_writers.find(data_item); //get mrw for data item
    
                        if (mrw_it == most_recent_writers.end()) { // data item not found 
                            //std::cout << "INSERT::WRITESET: key " << data_item.val << " not found" << std::endl;
                            continue;
                        }
    
                        if (mrw_it->second != nullptr) { // data item has mrw , check if mrw is in graph
    
                            // print transaction id
                            //std::cout << "INSERT::WRITESET: key " << data_item.val << " has mrw " << mrw_it->second->getUUID() << std::endl;
                            
                            if (graph.getNode(mrw_it->second->getUUID()) != nullptr) { // if mrw in graph
                                //std::cout << "INSERT::WRITESET:" << mrw_it->second->getUUID() << " in graph" << std::endl;
                                auto current_txn = graph.getNode(txn.getUUID());
                                current_txn->addNeighborOut(mrw_it->second);
                                //std::cout << "INSERT::WRITESET: adding edge from " << txn.getUUID() << " to " << mrw_it->second->getUUID() << std::endl;
                            }
    
                        }else {
                            //std::cout << "INSERT::WRITESET: key " << data_item.val << " has no mrw" << std::endl;
                        }
    
                        mrw_it->second = graph.getNode(txn.getUUID()); // set mrw to current transaction
                        
                        // readers ∩ Primary Set 
                        auto readers_it = most_recent_readers.find(data_item);
    
                        if (readers_it != most_recent_readers.end())
                        {
        
                            //std::cout << "INSERT::READERS: key " << data_item.val << " has readers" << std::endl;
        
                            auto readers = readers_it->second;
        
                            for (const auto &reader_id : readers)
                            {
                                
                                //std::cout << "INSERT::READERS: key " << data_item.val << " has reader " << reader_id << std::endl;
        
                                auto read_txn = graph.getNode(reader_id);
        
                                if (read_txn != nullptr) // if reader in graph
                                {
                                    auto current_txn = graph.getNode(txn.getUUID());
                                    current_txn->addNeighborOut(read_txn); // add reader to current transaction
                                    //std::cout << "INSERT::READERS: adding edge from " << txn.getUUID() << " to " << read_txn->getUUID() << std::endl;
                                }
                                
                            }
                            
                        }
    
                    }
    
    
                }
    
    
            }
    
    
        }
        
        //graph.printAll();
        graph.getMergedOrders_();

    }
    
}

Merger::Merger()
{

    partial_sequences.rehash(1);

    for (const auto& server : servers)
    {
        partial_sequences[server.id] = std::make_unique<Queue_TS<Transaction>>();
        expected_server_ids.push_back(server.id);
    }

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
    