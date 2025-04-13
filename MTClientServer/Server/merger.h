#ifndef MERGER_H
#define MERGER_H

#include <unordered_map>
#include <cstdint>
#include <string>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <vector>
#include<memory>

#include "transaction.h"
#include "queueTS.h"
#include "../proto/request.pb.h"

class Merger
{
private:
    pthread_t merger_thread;
    pthread_t popper;
    std::unordered_map<int32_t, std::unique_ptr<Queue_TS<request::Request>>> round_requests;
    std::unordered_map<int32_t, std::unique_ptr<std::vector<Transaction>>> partial_sequences;
    std::mutex round_mutex;
    std::condition_variable round_cv;
    
    // List of expected server IDs.
    std::vector<int32_t> expected_server_ids;

public:
    // Constructor receives the list of expected server ids.
    Merger();

    // Called by a peer-handling thread when a new request arrives.
    void processProtoRequest(const request::Request& req_proto);

    // Waits until one request per server is available, then processes them.
    void temp();

    // Processes one round of requests.
    void processRoundRequests();

    // Process local requests
    void processLocalRequests();
};

#endif // MERGER_H




