// graph.cpp
#include "graph.h"
#include <iostream>

Transaction* Graph::addNode(std::unique_ptr<Transaction> uptr) {
    const std::string& key = uptr->getUUID();
    Transaction* ptr = uptr.get();
    nodes[key] = std::move(uptr);
    return ptr;
}

Transaction* Graph::getNode(const std::string& uuid) {
    auto it = nodes.find(uuid);
    return it == nodes.end() ? nullptr : it->second.get();
}

void Graph::printAll() const {
    std::cout << "Graph contains " << nodes.size() << " node(s):\n";
    for (const auto& kv : nodes) {
        const Transaction* t = kv.second.get();
        // Basic info
        std::cout << "- UUID: "   << t->getUUID()
                  << ", order: "  << t->getOrder()
                  << ", client: " << t->getClientId() << "\n";
        // Operations
        const auto& ops = t->getOperations();
        std::cout << "    Operations (" << ops.size() << "):\n";
        for (size_t i = 0; i < ops.size(); ++i) {
            const auto& op = ops[i];
            std::cout << "      [" << i << "] "
                      << (op.type == OperationType::READ ? "READ" : "WRITE")
                      << " key=" << op.key;
            if (op.type == OperationType::WRITE) {
                std::cout << ", value=" << op.value;
            }
            std::cout << "\n";
        }
        // Neighbors
        const auto& nbr = t->getNeighbors();
        std::cout << "    Neighbors (" << nbr.size() << "):";
        for (auto *n : nbr) {
            std::cout << " " << n->getUUID();
        }
        std::cout << "\n";
    }
}

void Graph::clear() {
    nodes.clear();
}