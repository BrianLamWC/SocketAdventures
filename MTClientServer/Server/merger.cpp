#include "merger.h"
#include "utils.h"

void Merger::protoRequestToPartialSequence(const request::Request& req_proto){ //might need to

    // std::vector<Transaction>& server_transactions = partial_sequences[req_proto.server_id()]; //modify partial_sequences in place
    
    // for (const auto &txn : req_proto.transaction()) {
    //     std::vector<Operation> transaction = getOperationsFromProtoTransaction(txn);
    //     server_transactions.emplace_back(req_proto.client_id(), transaction); //apparently more efficient? creates vector in place instead of having to declare on in the first place
    // }

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