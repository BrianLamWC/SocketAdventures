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
    std::unordered_set<Transaction*> neighbors;
public:
    Transaction(int32_t order_, int32_t client_id_, const std::vector<Operation>& ops, const std::string& uuid_ = "")
        : order(order_), uuid(uuid_), client_id(client_id_), operations(ops) {}

    int32_t getOrder() const { return order; }
    const std::string& getUUID() const { return uuid; }
    int32_t getClientId() const { return client_id; }
    const std::vector<Operation>& getOperations() const { return operations; }

    void addNeighbor(Transaction* ptr) {
        neighbors.insert(ptr);
    }

    const std::unordered_set<Transaction*>& getNeighbors() const {
        return neighbors;
    }
};

#endif