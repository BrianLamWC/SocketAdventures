// graph.h
#ifndef GRAPH_H
#define GRAPH_H

#include <unordered_map>
#include <string>
#include <memory>
#include "transaction.h"

class Graph {
private:
    std::unordered_map<std::string, std::unique_ptr<Transaction>> nodes;

public:
    Transaction* addNode(std::unique_ptr<Transaction> uptr);
    Transaction* getNode(const std::string& uuid);

    /// Print every transaction in the graph:
    void printAll() const;

    /// Clear all nodes and reset the graph:
    void clear();
};

#endif // GRAPH_H
