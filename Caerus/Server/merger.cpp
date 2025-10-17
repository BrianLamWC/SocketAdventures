#include "merger.h"
#include "utils.h"

void Merger::popFromQueue()
{
    while (true)
    {
        // wait on the local queue’s CV, then pop
        request::Request req_proto;
        {
            std::unique_lock<std::mutex> local_lock(partial_sequencer_to_merger_queue_mtx);
            partial_sequencer_to_merger_queue_cv.wait(local_lock, []
                                                      { return !partial_sequencer_to_merger_queue_.empty(); });
            req_proto = partial_sequencer_to_merger_queue_.pop();
        }

        processRequest(req_proto);
    }
}

void Merger::processRequest(const request::Request &req_proto)
{

    if (req_proto.transaction_size() > 0)
    {
        // Log the request
        std::ofstream logf("./merger_logs/merger_log" + std::to_string(my_id) + ".jsonl", std::ios::app);
        if (logf)
        {
            logf << "{\"server\":" << req_proto.server_id()
                 << ",\"round\":" << req_proto.round()
                 << ",\"txns\":[";
            for (int i = 0; i < req_proto.transaction_size(); ++i)
            {
                logf << "\"" << req_proto.transaction(i).id() << "\"";
                if (i + 1 < req_proto.transaction_size())
                {
                    logf << ",";
                }
            }
            logf << "]}\n";
        }
        else
        {
            std::cerr << "MERGER: failed to open log file ./merger_logs/merger_log.jsonl\n";
        }
    }

    const int sid = req_proto.server_id();
    auto it = partial_sequences.find(sid);

    if (it == partial_sequences.end())
    {
        // unknown server id
        std::cerr << "MERGER: Unknown server ID " << req_proto.server_id() << " in processRequest" << std::endl;
        return;
    }

    auto &q = it->second; // get the Queue_TS<Transaction> for this server
    std::vector<Transaction> transactions;

    for (const auto &txn_proto : req_proto.transaction())
    {
        std::vector<Operation> operations = getOperationsFromProtoTransaction(txn_proto);

        Transaction txn(txn_proto.random_stamp(), req_proto.server_id(), operations, txn_proto.id());

        transactions.push_back(txn);

        q->push(transactions);
    }

    {
        std::lock_guard<std::mutex> g(ready_mtx);
        if (!enqueued_sids_.count(sid)) {
            enqueued_sids_.insert(sid);
            ready_q_.push_back(sid);
            ready_cv.notify_one();
        }
    }

}

std::ostream &operator<<(std::ostream &os, const Transaction &t)
{
    return os << "Txn{id=" << t.getUUID()
              << ", stamp=" << t.getOrder()
              << ", origin=" << t.getServerId()
              << ", ops=" << t.getOperations().size() << "}";
}

void Merger::dumpPartialSequences() const
{
    graph.printAll();
}

void Merger::insertAlgorithm()
{
    std::unique_lock<std::mutex> lk(ready_mtx);

    while (true)
    {
        ready_cv.wait(lk, [this]() { return !ready_q_.empty(); });

        int sid = ready_q_.front();
        ready_q_.pop_front();
        enqueued_sids_.erase(sid);
        lk.unlock();
        
        // printf("INSERT::Server %d\n", server.id);

        auto it = partial_sequences.find(sid);
        auto &inner_map = it->second;

        if (inner_map->empty()) {
            lk.lock(); // lock before going back to
            continue;
        }

        auto transactions = inner_map->pop();

        // print size and transction ids
        std::cout << "INSERT::Popped " << transactions.size() << " transactions from server " << sid << std::endl;
        for (const auto &txn : transactions)
        {
            std::cout << "  " << txn << std::endl;
        }

        std::unordered_set<DataItem> primary_set;
        std::unordered_map<DataItem, Transaction *> most_recent_writers;
        std::unordered_map<DataItem, std::unordered_set<std::string>> most_recent_readers;

        // setup the primary set for current server
        for (const auto &txn : transactions)
        {
            for (const auto &op : txn.getOperations())
            {
                auto db_it = mockDB.find(op.key);

                if (db_it == mockDB.end())
                {
                    std::cout << "INSERT::PrimarySet: key " << op.key << " not found" << std::endl;
                    continue;
                }

                auto data_item = db_it->second;

                if (data_item.primaryCopyID == sid)
                {
                    primary_set.insert(data_item);
                }

                // ensure the key exists in the MRW map (value initialized to nullptr)
                most_recent_writers.emplace(data_item, nullptr);
            }
        }

        for (auto &txn : transactions)
        {
            std::cout << "INSERT::Transaction: " << txn.getUUID() << std::endl;

            std::unordered_set<DataItem> write_set;
            std::unordered_set<DataItem> read_set;
            std::unordered_set<int32_t> expected_regions;

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
                }
                else if (op.type == OperationType::READ)
                {
                    read_set.insert(data_item);
                }

                expected_regions.insert(data_item.primaryCopyID); // add primary copy id to expected regions

                // pritn read and write set
                std::cout << "INSERT::ReadWriteSet: key " << op.key << " type " << (op.type == OperationType::READ ? "READ" : "WRITE") << std::endl;
            }

            auto curr_txn = graph.getNode(txn.getUUID());

            if (curr_txn == nullptr)
            {
                txn.setExpectedRegions(expected_regions);
                txn.addSeenRegion(sid);
                graph.addNode(std::make_unique<Transaction>(txn));
            }
            else
            {
                curr_txn->addSeenRegion(sid);
            }

            // Read Set ∩ Primary Set
            for (const auto &data_item : primary_set)
            {
                if (read_set.find(data_item) != read_set.end())
                {

                    // std::cout << "INSERT::READSET:" << data_item.val << " is in read and primary set" << std::endl;

                    auto mrw_it = most_recent_writers.find(data_item); // get mrw for data item

                    if (mrw_it == most_recent_writers.end())
                    { // data item not found
                        // std::cout << "INSERT::READSET: key " << data_item.val << " not found" << std::endl;
                        continue;
                    }

                    // auto &mrw_txn = mrw_it->second;

                    if (mrw_it->second != nullptr)
                    { // data item has mrw
                        // print transaction id
                        // std::cout << "INSERT::READSET: key " << data_item.val << " has mrw " << mrw_it->second->getUUID() << std::endl;

                        if (graph.getNode(mrw_it->second->getUUID()) != nullptr)
                        { // if mrw in graph
                            // std::cout << "INSERT::READSET:" << mrw_it->second->getUUID() << " in graph" << std::endl;
                            auto current_txn = graph.getNode(txn.getUUID());
                            current_txn->addNeighborOut(mrw_it->second);
                            // std::cout << "INSERT::READSET: adding edge from " << txn.getUUID() << " to " << mrw_it->second->getUUID() << std::endl;
                        }
                    }
                    else
                    {
                        // std::cout << "INSERT::READSET: key " << data_item.val << " has no mrw" << std::endl;
                    }

                    most_recent_readers[data_item].emplace(txn.getUUID()); // add current transaction to readers
                }
            }

            // Write Set ∩ Primary Set
            for (const auto &data_item : primary_set)
            {
                if (write_set.find(data_item) != write_set.end())
                {

                    std::cout << "INSERT::WRITESET:" <<data_item.val << " is in write and primary set" << std::endl;

                    auto mrw_it = most_recent_writers.find(data_item); // get mrw for data item

                    if (mrw_it == most_recent_writers.end())
                    { // data item not found
                        // std::cout << "INSERT::WRITESET: key " << data_item.val << " not found" << std::endl;
                        continue;
                    }

                    if (mrw_it->second != nullptr)
                    { // data item has mrw , check if mrw is in graph

                        // print transaction id
                        std::cout << "INSERT::WRITESET: key " << data_item.val << " has mrw " << mrw_it->second->getUUID() << std::endl;

                        if (graph.getNode(mrw_it->second->getUUID()) != nullptr)
                        { // if mrw in graph
                            std::cout << "INSERT::WRITESET:" << mrw_it->second->getUUID() << " in graph" << std::endl;
                            auto current_txn = graph.getNode(txn.getUUID());
                            current_txn->addNeighborOut(mrw_it->second);
                            std::cout << "INSERT::WRITESET: adding edge from " << txn.getUUID() << " to " << mrw_it->second->getUUID() << std::endl;
                        }
                    }
                    else
                    {
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
                                current_txn->addNeighborOut(read_txn); // add reader to current transaction
                                std::cout << "INSERT::READERS: adding edge from " << txn.getUUID() << " to " << read_txn->getUUID() << std::endl;
                            }
                        }
                    }
                }
            }
        }

    }
}

Merger::Merger()
{

    std::ofstream init_log("./merger_logs/merger_log" + std::to_string(my_id) + ".jsonl", std::ios::out | std::ios::trunc);
    // std::ofstream init_tp("throughput_log_" + std::to_string(my_id) + ".log", std::ios::out | std::ios::trunc);

    partial_sequences.reserve(servers.size());
    for (const auto &server : servers)
    {
        partial_sequences.emplace(server.id, std::make_unique<Queue_TS<std::vector<Transaction>>>());
        expected_server_ids.push_back(server.id);
    }

    // Create a popper thread that calls the popFromQueue() method.
    if (pthread_create(&popper, nullptr, [](void *arg) -> void *
                       {
            static_cast<Merger*>(arg)->popFromQueue();
            return nullptr; }, this) != 0)
    {
        threadError("Error creating merger thread");
    }

    pthread_detach(popper);

    // Create an insert thread that calls the insertAlgorithm() method.
    if (pthread_create(&insert_thread, nullptr, [](void *arg) -> void *
                       {
            static_cast<Merger*>(arg)->insertAlgorithm();
            return nullptr; }, this) != 0)
    {
        threadError("Error creating insert thread");
    }

    pthread_detach(insert_thread);

    // Create an dump thread that calls the dumpPartialSequences() method every 10 seconds.
    if (pthread_create(&dump_thread, nullptr, [](void *arg) -> void *
                       {
            while (true)
            {
                sleep(10);
                static_cast<Merger*>(arg)->dumpPartialSequences();
            }
            return nullptr; }, this) != 0)
    {
        threadError("Error creating dump thread");
    }
    pthread_detach(dump_thread);
}
