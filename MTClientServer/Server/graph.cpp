// graph.cpp
#include "graph.h"
#include <iostream>
#include <queue>
#include <algorithm>

Transaction *Graph::addNode(std::unique_ptr<Transaction> uptr)
{
    const std::string &key = uptr->getUUID();
    Transaction *ptr = uptr.get();
    nodes[key] = std::move(uptr);
    return ptr;
}

Transaction *Graph::getNode(const std::string &uuid)
{
    auto it = nodes.find(uuid);
    return it == nodes.end() ? nullptr : it->second.get();
}

void Graph::printAll() const
{
    std::cout << "Graph contains " << nodes.size() << " node(s):\n";
    for (const auto &kv : nodes)
    {
        const Transaction *t = kv.second.get();
        // Basic info
        std::cout << "- UUID: " << t->getUUID()
                  << ", order: " << t->getOrder()
                  << ", client: " << t->getClientId() << "\n";
        // Operations
        const auto &ops = t->getOperations();
        std::cout << "    Operations (" << ops.size() << "):\n";
        for (size_t i = 0; i < ops.size(); ++i)
        {
            const auto &op = ops[i];
            std::cout << "      [" << i << "] "
                      << (op.type == OperationType::READ ? "READ" : "WRITE")
                      << " key=" << op.key;
            if (op.type == OperationType::WRITE)
            {
                std::cout << ", value=" << op.value;
            }
            std::cout << "\n";
        }
        // Neighbors
        const auto &nbr = t->getNeighbors();
        std::cout << "    Neighbors (" << nbr.size() << "):";
        for (auto *n : nbr)
        {
            std::cout << " " << n->getUUID();
        }
        std::cout << "\n";

        // Expected regions
        const auto &expected_regions = t->getExpectedRegions();
        std::cout << "    Expected regions (" << expected_regions.size() << "):";
        for (auto region : expected_regions)
        {
            std::cout << " " << region;
        }
        std::cout << "\n";

        // Seen regions
        const auto &seen_regions = t->getSeenRegions();
        std::cout << "    Seen regions (" << seen_regions.size() << "):";
        for (auto region : seen_regions)
        {
            std::cout << " " << region;
        }
        std::cout << "\n";
    }
}

void Graph::clear()
{
    nodes.clear();
    index_map.clear();
    low_link_map.clear();
    tarjan_stack = std::stack<Transaction *>{};
    on_stack.clear();
    sccs.clear();
    current_index = 0;
}

std::unique_ptr<Transaction> Graph::removeTransaction(Transaction *rem)
{
    // 1) Find the map entry
    auto it = nodes.find(rem->getUUID());
    if (it == nodes.end())
    {
        return nullptr; // no such node
    }

    // 2) Remove rem from everyone else's adjacency
    for (auto &kv : nodes)
    {
        kv.second->removeNeighbor(rem);
    }

    // 3) Steal the unique_ptr<Transaction> out of the map
    std::unique_ptr<Transaction> up = std::move(it->second);

    // 4) Erase the entry so there's no null-pointer left behind
    nodes.erase(it);

    // 5) Return ownership to the caller
    return up;
}

void Graph::strongConnect(Transaction *v)
{
    // 1. Set the depth index for v
    index_map[v] = current_index;
    low_link_map[v] = current_index;
    ++current_index;

    tarjan_stack.push(v);
    on_stack.insert(v);

    // 2. Consider successors of v
    for (Transaction *w : v->getNeighbors())
    {
        if (index_map.find(w) == index_map.end())
        {
            // w has not yet been visited; recurse on it
            strongConnect(w);
            low_link_map[v] = std::min(low_link_map[v], low_link_map[w]);
        }
        else if (on_stack.count(w))
        {
            // successor w is in stack and hence in the current SCC
            low_link_map[v] = std::min(low_link_map[v], index_map[w]);
        }
    }

    // 3. If v is a root node, pop the stack and generate an SCC
    if (low_link_map[v] == index_map[v])
    {
        std::vector<Transaction *> component;
        Transaction *w = nullptr;
        do
        {
            w = tarjan_stack.top();
            tarjan_stack.pop();
            on_stack.erase(w);
            component.push_back(w);
        } while (w != v);
        sccs.push_back(std::move(component));
    }
}

void Graph::findSCCs()
{
    // reset any old Tarjan state
    index_map.clear();
    low_link_map.clear();
    while (!tarjan_stack.empty())
        tarjan_stack.pop();
    on_stack.clear();
    sccs.clear();
    current_index = 0;

    // run Tarjan on every node
    for (auto &kv : nodes)
    {
        Transaction *v = kv.second.get();
        if (index_map.find(v) == index_map.end())
        {
            strongConnect(v);
        }
    }

    // print the SCCs
    // std::cout << "Found " << sccs.size() << " strongly connected component(s):\n";
    // for (size_t i = 0; i < sccs.size(); ++i) {
    //     auto& comp = sccs[i];
    //     std::cout << " Component " << i+1 << " (size=" << comp.size() << "):\n";
    //     for (Transaction* tx : comp) {
    //         std::cout
    //             << "   - UUID: "   << tx->getUUID()
    //             << ", order: "    << tx->getOrder()
    //             << ", client: "   << tx->getClientId()
    //             << "\n";
    //     }
    // }
}

void Graph::buildTransactionSCCMap()
{

    txn_scc_index_map.clear();

    for (int i = 0; i < (int)sccs.size(); ++i)
    {
        for (Transaction *txn_ptr : sccs[i])
        {
            txn_scc_index_map[txn_ptr] = i;
        }
    }

    // // Print the mapping
    // std::cout << "Transaction to SCC mapping:\n";
    // for (const auto& pair : txn_scc_index_map) {
    //     std::cout << "Transaction UUID: " << pair.first->getUUID()
    //               << " is in SCC: " << pair.second << "\n";
    // }
}

void Graph::buildCondensationGraph()
{
    // 1. Resize to one entry per SCC, and clear any old edges
    neighbors_out.assign(sccs.size(), {});
    neighbors_in.assign(sccs.size(), {});

    // 2. Walk *every* original edge u → v in your graph:
    for (auto &kv : nodes)
    {
        Transaction *u = kv.second.get();
        int cu = txn_scc_index_map[u]; // which SCC u belongs to

        for (Transaction *v : u->getNeighbors())
        {
            int cv = txn_scc_index_map[v]; // which SCC v belongs to

            // 3. If it crosses SCCs, record it once
            if (cu != cv)
            {
                // neighbors_out[cu].insert(cv).second is true only on the first insertion
                if (neighbors_out[cu].insert(cv).second)
                {
                    // record the reverse link too
                    neighbors_in[cv].push_back(cu);
                }
            }
        }
    }
}

bool Graph::isSCCComplete(const int &scc_index)
{
    // For SCC c to be a sink, *no* member may have an edge out to a txn in a different comp.
    // And for completeness, each txn must return true from isComplete().
    for (auto *T : sccs[scc_index])
    {
        // 1) SCC‐sink check: scan every outgoing edge of T
        for (auto *nbr : T->getNeighbors())
        {
            if (txn_scc_index_map.at(nbr) != scc_index)
            {
                // there is an outgoing edge from this SCC to another SCC
                return false;
            }
        }
        // 2) per‐txn completeness
        if (!T->isComplete())
        {
            return false;
        }
    }
    // if we get here, no crossing edges *and* every T is complete
    return true;
}

void Graph::getMergedOrders()
{

    // instead of seeding every node, seed one rep per SCC:
    findSCCs();
    buildTransactionSCCMap();
    buildCondensationGraph();

    std::queue<std::string> Q;
    std::vector<std::unique_ptr<Transaction>> S;

    for (int c = 0; c < (int)sccs.size(); ++c)
    {
        // pick the first txn in each SCC as its “rep”
        Q.push(sccs[c][0]->getUUID());
    }

    while (!Q.empty())
    {
        auto *txn_ptr = getNode(Q.front());
        Q.pop();

        if (txn_ptr == nullptr)
        {
            continue;
        }

        findSCCs();
        buildTransactionSCCMap();
        buildCondensationGraph();

        int scc_index = txn_scc_index_map[txn_ptr];

        // check sink + completeness
        bool allDone = neighbors_out[scc_index].empty();
        for (auto *X : sccs[scc_index])
        {
            if (!X->isComplete())
            {
                allDone = false;
                break;
            }
        }

        if (allDone)
        {
            // emit entire SCC, in UUID order
            auto &scc = sccs[scc_index];
            std::sort(scc.begin(), scc.end(), [&](auto *a, auto *b)
                      { return a->getOrder() < b->getOrder(); });
            for (auto *txn : scc)
            {
                auto up = removeTransaction(txn);
                if (up)
                    S.push_back(std::move(up));
            }
            // re‐enqueue each predecessor SCC’s rep
            for (int predC : neighbors_in[scc_index])
            {
                Q.push(sccs[predC][0]->getUUID());
            }
        }
    }

    // print S
    std::cout << "Merged orders:\n";
    for (const auto &txn : S)
    {
        std::cout
            << "- UUID: " << txn->getUUID()
            << " order: " << txn->getOrder()
            << ", client: " << txn->getClientId() << "\n";
    }
}

void Graph::getMergedOrders_()
{

    // 1) SCC + condensation once
    findSCCs();
    buildTransactionSCCMap();
    buildCondensationGraph();
    int C = sccs.size();

    // 2) compute out-degrees and seed ready queue
    std::vector<int> out_degrees(C);
    std::queue<int> Q;

    for (int c = 0; c < C; ++c)
    {
        out_degrees[c] = neighbors_out[c].size();
    }

    for (int c = 0; c < C; ++c)
    {
        if (out_degrees[c] == 0 && isSCCComplete(c))
        {
            Q.push(c);
        }
    }

    // 3) peel off SCCs
    std::vector<std::unique_ptr<Transaction>> S;
    while (!Q.empty())
    {
        int c = Q.front();
        Q.pop();

        auto &comp = sccs[c];

        if (comp.size() > 1){
            std::sort(comp.begin(), comp.end(), [&](auto *a, auto *b) { return a->getOrder() < b->getOrder(); });
        }

        // emit & remove
        for (auto *T : comp)
        {
            auto up = removeTransaction(T);
            if (up)
                S.push_back(std::move(up));
        }

        // update predecessors
        for (int p : neighbors_in[c])
        {

            if (--out_degrees[p] == 0 && isSCCComplete(p))
            {
                Q.push(p);
            }
        }
    }

    // std::cout << "Merged orders:\n";
    // for (const auto &txn : S)
    // {
    //     std::cout
    //         << "- UUID: " << txn->getUUID()
    //         << " ORDER: " << txn->getOrder()
    //         << " lamport_stamp: " << txn->getLamportStamp() << "\n";
    // }
}