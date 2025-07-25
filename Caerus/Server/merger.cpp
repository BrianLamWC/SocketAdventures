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

            processIncomingRequest2(req_proto);
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

void Merger::processIncomingRequest2(const request::Request& req_proto) {
  if (!req_proto.has_server_id() || !req_proto.has_round()) return;
  int sid = req_proto.server_id(), r = req_proto.round();

  std::lock_guard<std::mutex> lk(round_mutex);

  // buffer every arrival
  batchBuffer[sid][r] = req_proto;

  // try to drain all complete rounds
  while (true) {
    int exp = nextExpectedBatch[sid];

    // check if *every* server has batch `exp`
    bool haveAll = true;
    for (int other : expected_server_ids) {
      if (!batchBuffer[other].count(exp)) {
        haveAll = false;
        break;
      }
    }
    if (!haveAll) break;

    // assemble this round
    current_batch.clear();
    current_batch.reserve(expected_server_ids.size());
    for (int other : expected_server_ids) {
      current_batch.push_back(std::move(batchBuffer[other][exp]));
      batchBuffer[other].erase(exp);
    }

    // bump every server’s expected counter
    for (int other : expected_server_ids) {
      nextExpectedBatch[other]++;
    }

    // --- LOG TO JSONL BEFORE PROCESSING ---
    {
      std::ofstream logf(log_path_, std::ios::app);
      if (!logf) {
        std::cerr << "Merger: failed to open log file " << log_path_ << "\n";
      } else {
        for (auto &batch : current_batch) {
          // {"server":X,"round":Y,"txns":[a,b,c]}
          logf << "{\"server\":" << batch.server_id()
               << ",\"round\":"  << exp
               << ",\"txns\":[";
          for (int i = 0; i < batch.transaction_size(); ++i) {
            logf << batch.transaction(i).id();
            if (i + 1 < batch.transaction_size()) logf << ",";
          }
          logf << "]}\n";
        }
      }
    }

    // now do your normal processing, dropping the lock during heavy work
    {
      std::unique_lock<std::mutex> ul(round_mutex, std::adopt_lock);
      ul.unlock();

      processRoundRequests();

      {
        std::lock_guard<std::mutex> g(insert_mutex);
        round_ready = true;
      }
      insert_cv.notify_one();

      ul.lock();
    }
  }
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

    std::ofstream init_log(log_path_, std::ios::out | std::ios::trunc);

    partial_sequences.rehash(1);

    for (const auto& server : servers)
    {
        partial_sequences[server.id] = std::make_unique<Queue_TS<Transaction>>();
        expected_server_ids.push_back(server.id);
        nextExpectedBatch[server.id] = 0;
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