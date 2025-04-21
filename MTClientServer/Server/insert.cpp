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

    // t1: Write to A, then Write to B
    std::vector<Operation> t1_ops = {
        {OperationType::WRITE, "1", "value_for_1_t1"},
        {OperationType::WRITE, "2", "value_for_2_t1"}};

    // t2: Write to A, then Write to B, then Write to C
    std::vector<Operation> t2_ops = {
        {OperationType::WRITE, "1", "value_for_1_t2"},
        {OperationType::WRITE, "2", "value_for_2_t2"},
        {OperationType::WRITE, "3", "value_for_3_t2"}};

    // t3: Write to C only
    std::vector<Operation> t3_ops = {
        {OperationType::WRITE, "3", "value_for_3_t3"}};
    

    Transaction t1(1, 100, t1_ops, "uuid_t1");
    Transaction t2(2, 101, t2_ops, "uuid_t2");
    Transaction t3(3, 102, t3_ops, "uuid_t3");

    mock_partial_sequences[1] = std::make_unique<Queue_TS<Transaction>>();
    mock_partial_sequences[2] = std::make_unique<Queue_TS<Transaction>>();
    mock_partial_sequences[3] = std::make_unique<Queue_TS<Transaction>>();

    mock_partial_sequences[1]->push(t1);
    mock_partial_sequences[1]->push(t2); 


    mock_partial_sequences[2]->push(t2);
    mock_partial_sequences[2]->push(t1); 

    mock_partial_sequences[3]->push(t3);
    

}

void insertAlgorithm(){

    for (const auto &server : servers)
    {
        auto it = mock_partial_sequences.find(server.id);
        auto &inner_map = it->second;
        auto transactions = inner_map->popAll();

        std::vector<DataItem> primary_set;

        // setup the primary set for current server
        for (const auto &txn : transactions)
        {
            for (const auto &op : txn.getOperations())
            {
                auto it = mockDB.find(op.key);

                if (it == mockDB.end()) 
                {
                    std::cout << "INSERT::PrimarySet: key " << op.key <<" not found" << std::endl;
                    continue;
                }

                auto data_item = it->second;

                if ( data_item.primaryCopyID == server.id) // what happens if i do W(A)W(A)
                {
                    primary_set.push_back(data_item);
                }
            }
        }

        for (const auto &txn : transactions)
        {

            if (graph.getNode(txn.getUUID()) == nullptr) {
                graph.addNode(std::make_unique<Transaction>(txn));
            }

            std::unordered_map<DataItem, std::string> write_set;
            std::unordered_map<DataItem, std::string> read_set;

            for (const auto &op : txn.getOperations())
            {

                auto it = mockDB.find(op.key);

                if (it == mockDB.end())
                {
                    std::cout << "INSERT::ReadWriteSet: key " << op.key << " not found" << std::endl;
                    continue;
                }

                auto data_item = it->second;

                if (op.type == OperationType::WRITE)
                {
                    write_set[data_item] = nullptr;
                }else if (op.type == OperationType::READ){
                    write_set[data_item] = nullptr;
                }

            
            }
            


        }



    }
    
    graph.printAll();

}