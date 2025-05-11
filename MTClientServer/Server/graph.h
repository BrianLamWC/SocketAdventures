// graph.h
#ifndef GRAPH_H
#define GRAPH_H

#include <unordered_map>
#include <string>
#include <memory>
#include <vector>
#include <stack>
#include <unordered_set>

#include "transaction.h"

class Graph {
private:
    std::unordered_map<std::string, std::unique_ptr<Transaction>> nodes;

    // Tarjan’s helpers
    std::unordered_map<Transaction*, int> index_map, low_link_map;
    std::unordered_set<Transaction*> on_stack;
    std::stack<Transaction*> tarjan_stack;
    int currentIndex = 0;
    std::vector<std::vector<Transaction*>> sccs;

    void strongConnect(Transaction* v);

    // SCC helper
    std::unordered_map<Transaction*, int> txn_scc_index_map;

    // merged orders
    std::vector<int> merged_orders;

public:
    Transaction* addNode(std::unique_ptr<Transaction> uptr);
    Transaction* getNode(const std::string& uuid);

    void printAll() const;
    void clear();

    void findSCCs();
    void buildTransactionSCCMap();

    bool isSCCComplete(const int &scc_index);

    void getMergedOrders();

};

#endif // GRAPH_H
