#ifndef MERGER_H
#define MERGER_H

#include <unordered_map>
#include <cstdint>
#include <string>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <vector>
#include <queue>
#include<memory>

#include "transaction.h"
#include "queueTS.h"
#include "../proto/request.pb.h"
#include "graph.h"  

// Define a min-heap comparator for rounds
struct CompareByRound {
    bool operator()(const request::Request &a,
                    const request::Request &b) const {
        // priority_queue is a max-heap by default, so invert
        return a.round() > b.round();
    }
};

class Merger
{
private:
    // threads
    pthread_t merger_thread;
    pthread_t popper;
    pthread_t insert_thread;
    pthread_t remove_thread;

    int32_t last_round = INT32_MIN;
    const std::string log_path_ = "./logs/merger_log" + std::to_string(my_id) + ".jsonl";

    // For each round, map server_id → that server’s partial sequence
    std::unordered_map<int32_t, std::unordered_map<int32_t, request::Request>> pending_rounds;

    std::unordered_map<int32_t, std::priority_queue< request::Request, std::vector<request::Request>, CompareByRound>> pending_heaps;

    // When a full round is ready, we stash the batch here:
    std::map<int32_t, std::vector<request::Request>> ready_rounds;
    
    // Once a full round is ready, we stash the batch here:
    std::vector<request::Request> current_batch;

    // queues for round requests and partial sequences 
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


    std::unordered_map<int,int> nextExpectedBatch;
    std::unordered_map<int,std::map<int,request::Request>> batchBuffer;

    // throughput tracking
    int32_t total_transactions = 0;
    std::chrono::nanoseconds::rep ns_elapsed_time = 0;
    std::mutex total_transactions_mutex;


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
    void processIncomingRequest2(const request::Request& req_proto);
    void processIncomingRequest3(const request::Request& req_proto);

    void handleBatch(const request::Request &req);

    void calculateThroughput();

};


#endif // MERGER_H




