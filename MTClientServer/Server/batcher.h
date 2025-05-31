#ifndef BATCHER_H
#define BATCHER_H

#include <vector>
#include <cstdint>
#include <array>
#include <chrono>
#include <cstdio>
#include <random>
#include <mutex>
#include <condition_variable>
#include <random>
#include <limits>

#include "utils.h"
#include "transaction.h"
#include "queueTS.h"
#include "../proto/request.pb.h"

class Batcher
{
private:

    std::vector<request::Request> batch;
    pthread_t batcher_thread;

    pthread_t sender_thread;
    Queue_TS <request::Request> outbound_queue;
    std::mutex batch_mutex;
    std::condition_variable batch_cv;

    std::unordered_map<int, server> target_peers;
    std::unordered_map<int,int> partial_sequencer_fds; // id to fd mapping for partial sequencer connections

public:

    Batcher();
    void batchRequests();
    void processBatch(std::chrono::nanoseconds::rep &ns_total_stamp_time_);
    void sendTransaction(request::Request& txn);
    void sendTransactions();
    std::string uuidv7();

};

#endif // BATCHER_H