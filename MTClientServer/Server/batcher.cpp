#include "batcher.h"
#include "server.h"

void Batcher::batchRequests(){
    batch = requestQueue.popAll();

    if (!batch.empty()) {
        processBatch(batch);
    }


}

void Batcher::processBatch(const std::vector<Request>& batch){
    
    for (const Request &req : batch)
    {
        Transaction txn = req.transaction;
        printf("%s\n", txn);
    }
    
}