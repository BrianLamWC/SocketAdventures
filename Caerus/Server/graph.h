// graph.h
#ifndef GRAPH_H
#define GRAPH_H

#include <unordered_map>
#include <string>
#include <memory>
#include <vector>
#include <stack>
#include <unordered_set>
#include <mutex>
#include <condition_variable>

#include "transaction.h"
#include "queueTS.h"
#include "utils.h"
#include "../proto/graph_snapshot.pb.h"

class Graph
{
private:
    std::unordered_map<std::string, std::unique_ptr<Transaction>> nodes; // all nodes in the graph
    std::unordered_map<std::string, std::unique_ptr<Transaction>> nodes_static; // all nodes in the graph

    // Tarjan’s helpers
    std::unordered_map<Transaction *, int> index_map, low_link_map;
    std::unordered_set<Transaction *> on_stack;
    std::stack<Transaction *> tarjan_stack;
    int current_index = 0;
    std::vector<std::vector<Transaction *>> sccs;

    void strongConnect(Transaction *v);

    // SCC helper
    std::unordered_map<Transaction *, int> txn_scc_index_map; // maps each transaction to its SCC index
    std::vector<std::unordered_set<int>> neighbors_out;       // outgoing edges of each SCC (indexed by SCC index)
    std::vector<std::vector<int>> neighbors_in;               // incoming edges of each SCC (indexed by SCC index)
    mutable std::mutex mtx;                                   // protect nodes for snapshotting / external access

    Queue_TS<Transaction> merged;

public:
    Transaction *addNode(std::unique_ptr<Transaction> uptr);
    Transaction *getNode(const std::string &uuid);
    std::vector<Transaction*> getAllNodes() const;
    void addNeighborOut(Transaction* from, Transaction* to);

    void printAll() const;
    bool isEmpty() const { return nodes.empty(); }
    void clear();
    std::unique_ptr<Transaction> removeTransaction(Transaction *rem);

    void findSCCs();
    void buildTransactionSCCMap();
    void buildCondensationGraph();
    bool isSCCComplete(const int &scc_index);

    int32_t getMergedOrders_();

    // Build a GraphSnapshot protobuf message representing the current graph.
    // This will lock the graph while making a copy into the protobuf.
    void buildSnapshotProto(request::GraphSnapshot &out) const;
};

#endif // GRAPH_H
