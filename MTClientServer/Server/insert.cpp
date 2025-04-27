#include "insert.h"
#include "graph.h"
#include "utils.h"
#include "queueTS.h"
#include "transaction.h"
#include <functional> 

std::unordered_map<int32_t, std::unique_ptr<Queue_TS<Transaction>>> mock_partial_sequences;
Graph graph;

void factory(){

    //  Transaction transaction(txn_proto.order(), txn_proto.client_id(), operations, txn_proto.id());

    std::vector<Operation> t1_ops = {
        {OperationType::WRITE, "1", "value_for_1_t1"},};

    std::vector<Operation> t2_ops = {

        {OperationType::READ, "1", "value_for_1_t2"},};

    std::vector<Operation> t3_ops = {
        {OperationType::READ, "1", "value_for_1_t3"},};

    std::vector<Operation> t4_ops = {
        {OperationType::WRITE, "1", "value_for_1_t4"},};
    

    Transaction t1(1, 100, t1_ops, "uuid_t1");
    Transaction t2(2, 101, t2_ops, "uuid_t2");
    Transaction t3(3, 102, t3_ops, "uuid_t3");
    Transaction t4(4, 103, t4_ops, "uuid_t4");

    mock_partial_sequences[1] = std::make_unique<Queue_TS<Transaction>>();
    mock_partial_sequences[2] = std::make_unique<Queue_TS<Transaction>>();
    mock_partial_sequences[3] = std::make_unique<Queue_TS<Transaction>>();
    mock_partial_sequences[4] = std::make_unique<Queue_TS<Transaction>>();

    mock_partial_sequences[1]->push(t1);
    mock_partial_sequences[1]->push(t2);
    mock_partial_sequences[1]->push(t3);  
    mock_partial_sequences[1]->push(t4);


    // mock_partial_sequences[2]->push(t2);
    // mock_partial_sequences[2]->push(t1); 

    // mock_partial_sequences[3]->push(t2);
    // mock_partial_sequences[3]->push(t3);
    

}

void insertAlgorithm(){

    for (const auto &server : servers)
    {
        printf("INSERT::Server %d\n", server.id);

        auto it = mock_partial_sequences.find(server.id);
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
}