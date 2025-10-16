// graph.cpp

#include <iostream>
#include <queue>
#include <algorithm>

#include "graph.h"

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
    std::cout << "\033[2J\033[1;1H";
    std::cout << "Graph contains " << nodes.size() << " node(s):\n";
    for (const auto &kv : nodes)
    {
        const Transaction *t = kv.second.get();
        // Basic info
        std::cout << "- ID: " << t->getUUID() << "\n"
                  << "  Order: " << t->getOrder() << "\n";
        // Operations
        // const auto &ops = t->getOperations();
        // std::cout << "    Operations (" << ops.size() << "):\n";
        // for (size_t i = 0; i < ops.size(); ++i)
        // {
        //     const auto &op = ops[i];
        //     std::cout << "      [" << i << "] "
        //               << (op.type == OperationType::READ ? "READ" : "WRITE")
        //               << " key=" << op.key;
        //     if (op.type == OperationType::WRITE)
        //     {
        //         std::cout << ", value=" << op.value;
        //     }
        //     std::cout << "\n";
        // }
        // Neighbors
        const auto &nbr = t->getOutNeighbors();
        std::cout << "    Neighbors (" << nbr.size() << "):";
        for (auto *n : nbr)
        {
            std::cout << " " << n->getUUID();
        }
        std::cout << "\n\n";

        // // Expected regions
        // const auto &expected_regions = t->getExpectedRegions();
        // std::cout << "    Expected regions (" << expected_regions.size() << "):";
        // for (auto region : expected_regions)
        // {
        //     std::cout << " " << region;
        // }
        // std::cout << "\n";

        // // Seen regions
        // const auto &seen_regions = t->getSeenRegions();
        // std::cout << "    Seen regions (" << seen_regions.size() << "):";
        // for (auto region : seen_regions)
        // {
        //     std::cout << " " << region;
        // }
        // std::cout << "\n";
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
        kv.second->removeOutNeighbor(rem);
    }

    // 3) Steal the unique_ptr<Transaction> out of the map
    std::unique_ptr<Transaction> up = std::move(it->second);

    // 4) Erase the entry so there's no null-pointer left behind
    nodes.erase(it);

    // 5) Return ownership to the caller
    return up;
}

std::unique_ptr<Transaction> Graph::removeTransaction_(Transaction *rem)
{
    auto it = nodes.find(rem->getUUID());
    if (it == nodes.end())
        return nullptr;

    // copy the incoming‐neighbor list and break those edges
    std::vector<Transaction *> preds(
        rem->getIncomingNeighbors().begin(),
        rem->getIncomingNeighbors().end());
    for (Transaction *pred : preds)
    {
        pred->removeOutNeighbor(rem);
    }

    // copy the outgoing‐neighbor list and break those edges
    std::vector<Transaction *> succs(
        rem->getOutNeighbors().begin(),
        rem->getOutNeighbors().end());
    for (Transaction *succ : succs)
    {
        rem->removeOutNeighbor(succ);
    }

    // 3) now it’s safe to steal and erase
    auto up = std::move(it->second);
    nodes.erase(it);
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
    for (Transaction *w : v->getOutNeighbors())
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

    if (sccs.size() > 0)
    {
        // Print the SCCs
        std::cout << "Strongly Connected Components (SCCs):\n";
        for (size_t i = 0; i < sccs.size(); ++i)
        {
            std::cout << "Component " << i << ":";
            for (Transaction *t : sccs[i])
            {
                std::cout << " " << t->getUUID();
            }
            std::cout << "\n\n";
        }
    }
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

        for (Transaction *v : u->getOutNeighbors())
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
    // For SCC c to be a sink, no member may have an edge out to a txn in a different comp.
    // And for completeness, each txn must return true from isComplete().
    for (auto *T : sccs[scc_index])
    {
        // SCC‐sink check: scan every outgoing edge of T
        for (auto *nbr : T->getOutNeighbors())
        {
            if (txn_scc_index_map.at(nbr) != scc_index)
            {
                return false;
            }
        }

        if (!T->isComplete())
        {
            return false;
        }
    }

    return true;
}

int32_t Graph::getMergedOrders_()
{
    // 1) SCC + condensation once
    findSCCs(); // one rep per SCC
    buildTransactionSCCMap();
    buildCondensationGraph();
    int sccs_count = sccs.size();

    // 2) compute out-degrees and seed ready queue
    std::vector<int> out_degrees(sccs_count);
    std::queue<int> Q;

    for (int c = 0; c < sccs_count; ++c)
    {
        out_degrees[c] = neighbors_out[c].size();
    }

    for (int c = 0; c < sccs_count; ++c)
    {
        if (out_degrees[c] == 0 && isSCCComplete(c))
        {
            Q.push(c);
        }
    }

    int32_t transaction_count = 0;

    while (!Q.empty())
    {
        int c = Q.front();
        Q.pop();
        auto &comp = sccs[c];

        if (comp.size() > 1)
        {

            std::sort(comp.begin(), comp.end(),
                      [&](auto *a, auto *b)
                      {
                          int32_t a_rand = a->getOrder();
                          int32_t b_rand = b->getOrder();

                          if (a_rand != b_rand)
                          {
                              return a_rand < b_rand;
                          }

                          return a->getServerId() < b->getServerId();
                      });
        }

        for (Transaction *T : comp)
        {
            if (auto up = removeTransaction_(T))
            {
                merged_order.push(*up);
                transaction_count++;
            }
        }

        // // print merged orders
        // std::cout << "Merged " << comp.size() << " transactions from SCC "<< c << ":\n";
        // for (Transaction *T : comp) {
        //     std::cout << "  " << T->getUUID() << " (Order: " << T->getOrder() << ")\n";
        // }
        // std::cout << "\n\n";

        logging_cv.notify_all();

        for (int p : neighbors_in[c])
        {
            if (--out_degrees[p] == 0 && isSCCComplete(p))
            {
                Q.push(p);
            }
        }
    }

    return transaction_count;
}
