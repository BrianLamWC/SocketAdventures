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
    pthread_t remove_thread;

    // queues for round requests and partial sequences 
    std::unordered_map<int32_t, std::unique_ptr<Queue_TS<request::Request>>> round_requests;
    std::unordered_map<int32_t, std::unique_ptr<Queue_TS<Transaction>>> partial_sequences;

    // For each round-ID, map server_id → that server’s Request
    std::map<int32_t, std::unordered_map<int32_t, request::Request>> pending_rounds;

    // Once a full round is ready, we stash the batch here:
    std::vector<request::Request> current_batch;

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

    // Processes one round of requests.
    void processRoundRequests();

    // Process local requests
    void processLocalRequests();

    // Insert algorithm
    void insertAlgorithm();

    void processIncomingRequest(const request::Request& req_proto);
};


#endif // MERGER_H




