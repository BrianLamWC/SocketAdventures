#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <string>
#include <vector>

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
    std::string client_id;
    std::vector<Operation> operations;
public:
    // Constructor to initialize a Transaction
    Transaction(std::string client_id, const std::vector<Operation>& ops)
        : client_id(client_id), operations(ops) {}

    // Getters to access client ID and operations
    std::string getClientId() const { return client_id; }
    const std::vector<Operation>& getOperations() const { return operations; }
};

#endif