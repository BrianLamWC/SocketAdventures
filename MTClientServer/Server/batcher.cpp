#include "batcher.h"
#include "server.h"
#include <unistd.h>
#include "utils.h"
#include <cstring>

void Batcher::batchRequests()
{

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

void Batcher::processBatch(const std::vector<Transaction>& batch)
{
    
    for (const Transaction &txn : batch)
    {
        std::vector<Operation> operations = txn.getOperations();

        printf("(BATCHER) Transaction for client %d:\n", txn.getClientId());

        for (const auto& op : operations) {
            // Convert the enum type to string
            std::string operationTypeStr = (op.type == OperationType::WRITE) ? "WRITE" : "READ";
            
            // For write operations, print the value as well
            if (op.type == OperationType::WRITE) {
                printf("  Operation: %s, Key: %s, Value: %s\n", operationTypeStr.c_str(), op.key.c_str(), op.value.c_str());
            } else {
                // For read operations, value is not printed
                printf("  Operation: %s, Key: %s\n", operationTypeStr.c_str(), op.key.c_str());
            }
        }
        
    }
    
}

// Constructor
Batcher::Batcher()
{

    if (pthread_create(&batcher_thread, nullptr, [](void* arg) -> void* {
            static_cast<Batcher*>(arg)->batchRequests();
            return nullptr;
        }, this) != 0) 
    {
        threadError("Error creating batcher thread");
    }
    
    pthread_detach(batcher_thread);

}
