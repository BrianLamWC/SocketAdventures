#ifndef BATCHER_H
#define BATCHER_H

#include <vector>
#include <cstdint>
#include <array>
#include <chrono>
#include <cstdio>
#include <random>

#include "utils.h"
#include "transaction.h"
#include "queueTS.h"
#include "../proto/request.pb.h"

class Batcher
{
private:

    std::vector<request::Request> batch_;
    pthread_t batcher_thread;
    static const int BATCH_SIZE = 10;

public:

    Batcher();
    void batchRequests();
    void processBatch_();
    void sendTransaction_(const request::Transaction& txn, const int32_t& id);
    std::string uuidv7();

};

#endif