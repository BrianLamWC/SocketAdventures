#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <string>
#include <vector>
#include <cstdint>
#include <cstring>
#include <unordered_set>

enum class OperationType {
    READ,
    WRITE
};

struct Operation {
    OperationType type;
    std::string key;
    std::string value;  // Only used for write operations
};

class Transaction
{
private:
    int32_t order;
    std::string uuid;
    int32_t client_id;
    std::vector<Operation> operations;

    // intrusive adjacency list
    std::unordered_set<Transaction*> neighbors_out;
    std::unordered_set<Transaction*> neighbors_in;

    std::unordered_set<int32_t> expected_regions;
    std::unordered_set<int32_t> seen_regions;
public:
    Transaction(int32_t order_, int32_t client_id_, const std::vector<Operation>& ops, const std::string& uuid_ = "")
        :order(order_), uuid(uuid_), client_id(client_id_), operations(ops) {}


    int32_t getOrder() const { return order; }

    const std::string& getUUID() const { return uuid; }

    int32_t getClientId() const { return client_id; }

    const std::vector<Operation>& getOperations() const { return operations; }

    void addNeighborOut(Transaction* ptr) { 
        neighbors_out.insert(ptr); 
        ptr->neighbors_in.insert(this); 
    }

    void removeOutNeighbor(Transaction* ptr) { 
        neighbors_out.erase(ptr); 
        ptr->neighbors_in.erase(this);
    }

    void removeInNeighbor(Transaction* ptr) { 
        neighbors_in.erase(ptr); 
        ptr->neighbors_out.erase(this);
    }

    const std::unordered_set<Transaction*>& getOutNeighbors() const { return neighbors_out; }

    const std::unordered_set<Transaction*>& getIncomingNeighbors() const { return neighbors_in; }

    void setExpectedRegions(const std::unordered_set<int32_t>& regions) { expected_regions = regions; }
    const std::unordered_set<int32_t>& getExpectedRegions() const { return expected_regions; }

    void addSeenRegion(int32_t region) { seen_regions.insert(region); }
    const std::unordered_set<int32_t>& getSeenRegions() const { return seen_regions; }

    bool isComplete() const { return seen_regions == expected_regions; }


    
};

#endif