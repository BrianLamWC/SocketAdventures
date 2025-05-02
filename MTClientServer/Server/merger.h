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
#include "graph.h"  

class Merger
{
private:
    // threads
    pthread_t merger_thread;
    pthread_t popper;
    pthread_t insert_thread;

    // queues for round requests and partial sequences 
    std::unordered_map<int32_t, std::unique_ptr<Queue_TS<request::Request>>> round_requests;
    std::unordered_map<int32_t, std::unique_ptr<Queue_TS<Transaction>>> partial_sequences;

    // mutexes 
    std::mutex round_mutex;
    std::condition_variable round_cv;
    std::mutex insert_mutex;
    std::condition_variable insert_cv;
    bool round_ready = false;

    // List of expected server IDs.
    std::vector<int32_t> expected_server_ids;

    // Copy of the graph
    Graph graph;

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

    // Insert algorithm
    void insertAlgorithm();
};


#endif // MERGER_H




