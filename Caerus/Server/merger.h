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
#include <memory>

#include "transaction.h"
#include "queueTS.h"
#include "../proto/request.pb.h"
#include "../proto/graph_snapshot.pb.h"
#include "graph.h"

// Define a min-heap comparator for rounds
struct CompareByRound
{
    bool operator()(const request::Request &a,
                    const request::Request &b) const
    {
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
    pthread_t dump_thread;

    // map server_id → queue of Transactions
    std::unordered_map<int32_t, std::unique_ptr<Queue_TS<std::vector<Transaction>>>> partial_sequences;

    // mutexes
    std::mutex ready_mtx;
    std::condition_variable ready_cv;

    // List of expected server IDs.
    std::vector<int32_t> expected_server_ids;
    std::deque<int> ready_q_;               // which server ids need processing
    std::unordered_set<int> enqueued_sids_; // coalesce: sid is already in ready_q_

    // Copy of the graph
    Graph graph;
    std::mutex graph_mtx;

public:
    // Constructor receives the list of expected server ids.
    Merger();

    // Processes one round of requests.
    void processRoundRequests();

    // Pop from input queue
    void popFromQueue();

    // Process a single incoming request
    void processRequest(const request::Request &req_proto);

    void processIncomingRequest(const request::Request &req_proto);

    void dumpPartialSequences() const;

    // Insert algorithm
    void insertAlgorithm();

    // Send a graph snapshot (fram ed protobuf) to the given file descriptor.
    void sendGraphSnapshotOnFd(int fd);
    // Send merged orders (as a Request with recipient MERGED_ORDER) to the given fd.
    void sendMergedOrdersOnFd(int fd);
};

#endif // MERGER_H
