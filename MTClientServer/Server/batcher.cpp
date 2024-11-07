#include "batcher.h"
#include "server.h"
#include <unistd.h>
#include "utils.h"
#include <cstring>

void Batcher::batchRequests(){

    while (true)
    {
        batch = requestQueue.popAll();

        if (!batch.empty()) {
            processBatch(batch);
        }

        batch.clear();

        sleep(5);
    }
    
    pthread_exit(NULL);

}

void Batcher::processBatch(const std::vector<Request>& batch){
    
    for (const Request &req : batch)
    {
        Transaction txn = req.transaction;
        printf("BATCHER: %s\n", txn.data.c_str());
    }
    
    
}

// Constructor
Batcher::Batcher() {

    if (pthread_create(&batcher_thread, nullptr, [](void* arg) -> void* {
            static_cast<Batcher*>(arg)->batchRequests();
            return nullptr;
        }, this) != 0) 
    {
        threadError("Error creating batcher thread");
    }
    
    pthread_detach(batcher_thread);

}
