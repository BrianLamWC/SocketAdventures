#ifndef BATCHER_H
#define BATCHER_H

#include <vector>
#include <cstdint>

#include "server.h"
#include "utils.h"
#include "transaction.h"

class Batcher
{
private:

    std::vector<Transaction> batch; 
    pthread_t batcher_thread;
    static const int BATCH_SIZE = 10;

public:

    Batcher();
    void batchRequests();
    void processBatch();
    void sendTransaction(const Transaction& txn, const int32_t& id);

};

#endif