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

        // Expected regions
        const auto& expected_regions = t->getExpectedRegions();
        std::cout << "    Expected regions (" << expected_regions.size() << "):";
        for (auto region : expected_regions) {
            std::cout << " " << region;
        }
        std::cout << "\n";

        // Seen regions
        const auto& seen_regions = t->getSeenRegions();
        std::cout << "    Seen regions (" << seen_regions.size() << "):";
        for (auto region : seen_regions) {
            std::cout << " " << region;
        }
        std::cout << "\n";

    }
}

void Graph::clear() {
    nodes.clear();
    // reset Tarjan state too, if you plan to reuse the same Graph
    index_map.clear();
    low_link_map.clear();
    stack = std::stack<Transaction*>{};
    on_stack.clear();
    sccs.clear();
    currentIndex = 0;
}

void Graph::strongConnect(Transaction* v) {
    // 1. Set the depth index for v
    index_map[v] = currentIndex;
    low_link_map[v]  = currentIndex;
    ++currentIndex;

    tarjan_stack.push(v);
    on_stack.insert(v);

    // 2. Consider successors of v
    for (Transaction* w : v->getNeighbors()) {
        if (index_map.find(w) == index_map.end()) {
            // w has not yet been visited; recurse on it
            strongConnect(w);
            low_link_map[v] = std::min(low_link_map[v], low_link_map[w]);
        }
        else if (on_stack.count(w)) {
            // successor w is in stack and hence in the current SCC
            low_link_map[v] = std::min(low_link_map[v], index_map[w]);
        }
    }

    // 3. If v is a root node, pop the stack and generate an SCC
    if (low_link_map[v] == index_map[v]) {
        std::vector<Transaction*> component;
        Transaction* w = nullptr;
        do {
            w = stack.top(); stack.pop();
            on_stack.erase(w);
            component.push_back(w);
        } while (w != v);
        sccs.push_back(std::move(component));
    }
}

void Graph::findSCCs() {
    // reset any old Tarjan state
    index_map.clear();
    low_link_map.clear();
    while (!tarjan_stack.empty()) tarjan_stack.pop();
    on_stack.clear();
    sccs.clear();
    currentIndex = 0;

    // run Tarjan on every node
    for (auto& kv : nodes) {
        Transaction* v = kv.second.get();
        if (index_map.find(v) == index_map.end()) {
            strongConnect(v);
        }
    }

    // print the SCCs
    std::cout << "Found " << sccs.size() << " strongly connected component(s):\n";
    for (size_t i = 0; i < sccs.size(); ++i) {
        auto& comp = sccs[i];
        std::cout << " Component " << i+1 << " (size=" << comp.size() << "):\n";
        for (Transaction* tx : comp) {
            std::cout 
                << "   - UUID: "   << tx->getUUID()
                << ", order: "    << tx->getOrder()
                << ", client: "   << tx->getClientId() 
                << "\n";
        }
    }

}

void Graph::buildTransactionSCCMap(){

    for (int i = 0; i < (int)sccs.size(); ++i) {
    for (Transaction* txn_ptr : sccs[i]) {
        txn_scc_index_map[txn_ptr] = i;
    }
    }

    // Print the mapping
    std::cout << "Transaction to SCC mapping:\n";
    for (const auto& pair : txn_scc_index_map) {
        std::cout << "Transaction UUID: " << pair.first->getUUID() 
                  << " is in SCC: " << pair.second << "\n";
    }
    
}

bool Graph::isSCCComplete(const int &scc_index)
{
  // For SCC c to be a sink, *no* member may have an edge out to a txn in a different comp.
  // And for completeness, each txn must return true from isComplete().
  for (auto* T : sccs[scc_index]) {
    // 1) SCC‐sink check: scan every outgoing edge of T
    for (auto* nbr : T->getNeighbors()) {
      if ( txn_scc_index_map.at(nbr) != scc_index ) {
        // there is an outgoing edge from this SCC to another SCC
        return false;
      }
    }
    // 2) per‐txn completeness
    if (!T->isComplete()) {
      return false;
    }
  }
  // if we get here, no crossing edges *and* every T is complete
  return true;
}

void Graph::getMergedOrders()
{

    sccs.clear();
    txn_scc_index_map.clear();
    merged_orders.clear();

    findSCCs();
    buildTransactionSCCMap();


    for (int c = 0; c < (int)sccs.size(); ++c) {
        if (isSCCComplete(c)) {
            merged_orders.push_back(c);
        }
    }

}
