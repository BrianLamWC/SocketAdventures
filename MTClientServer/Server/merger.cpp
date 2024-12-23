#include "merger.h"

#include "utils.h"

void Merger::protoToPartialSequence(const request::Request& req_proto){

    std::vector<Transaction> partial_sequence;
    
    for (const auto &txn : req_proto.transaction())
    {
        std::vector<Operation> operations = getOperationsFromProtoTransaction(txn);

        Transaction transaction(req_proto.client_id(), operations);

        partial_sequence.push_back(transaction);

    }
    

}

void Merger::temp()
{

    while (true)
    {
        sleep(5);
    }
    

}

Merger::Merger()
{

    for (const auto& server : servers)
    {
        partial_sequences[server.id] = std::vector<Transaction>();
    }
    

    if (pthread_create(&merger_thread, NULL, [](void* arg) -> void* {
            static_cast<Merger*>(arg)->Merger::temp();
            return nullptr;
        }, this) != 0) 
    {
        threadError("Error creating batcher thread");
    }
    
    pthread_detach(merger_thread);    

}