#include "utils.h"
#include "queueTS.h"
#include "transaction.h"

std::unordered_map<int32_t, std::unique_ptr<Queue_TS<Transaction>>> mock_partial_sequences;

void insertAlgorithm(){

    for (const auto &server : servers)
    {
        auto it = mock_partial_sequences.find(server.id);
        auto &inner_map = it->second;
        auto transactions = inner_map->popAll();


        for (const auto &txn : transactions)
        {
            
        }
        



    }
    


}