#include "utils.h"
#include "queueTS.h"
#include "transaction.h"

std::unordered_map<int32_t, std::unique_ptr<Queue_TS<Transaction>>> mock_partial_sequences;

void factory(){

    //  Transaction transaction(txn_proto.order(), txn_proto.client_id(), operations, txn_proto.id());

    // t1: Write to A, then Write to B
    std::vector<Operation> t1_ops = {
        {OperationType::WRITE, "A", "value_for_A_t1"},
        {OperationType::WRITE, "B", "value_for_B_t1"}};

    // t2: Write to A, then Write to B, then Write to C
    std::vector<Operation> t2_ops = {
        {OperationType::WRITE, "A", "value_for_A_t2"},
        {OperationType::WRITE, "B", "value_for_B_t2"},
        {OperationType::WRITE, "C", "value_for_C_t2"}};

    // t3: Write to C only
    std::vector<Operation> t3_ops = {
        {OperationType::WRITE, "C", "value_for_C_t3"}};
    

    Transaction r1_t1(1, 100, t1_ops, "uuid_t1");
    Transaction r1_t2(2, 101, t2_ops, "uuid_t2");
    Transaction r1_t3(3, 102, t3_ops, "uuid_t3");


}

void insertAlgorithm(){

    for (const auto &server : servers)
    {
        auto it = mock_partial_sequences.find(server.id);
        auto &inner_map = it->second;
        auto transactions = inner_map->popAll();

        std::vector<DataItem> primary_set;
        std::vector<DataItem> write_set;
        std::vector<DataItem> read_set;

        for (const auto &txn : transactions)
        {
            for (const auto &op : txn.getOperations())
            {

                auto it = mockDB.find(op.key);

                if (it == mockDB.end())
                {
                    continue;
                }

                auto data_item = it->second;

                if ( data_item.primaryCopyID == server.id)
                {
                    primary_set.push_back(data_item);
                }
                
                
            }
            
        }
        

    }
    


}