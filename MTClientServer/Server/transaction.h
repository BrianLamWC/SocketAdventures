#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <string>
#include <vector>
#include <cstdint>
#include <cstring>

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
    char uuid_str[37];
    int32_t client_id;
    std::vector<Operation> operations;
public:
    // Constructor to initialize a Transaction
    Transaction(const char* id, int32_t client_id, const std::vector<Operation>& ops) : client_id(client_id), operations(ops)
    {
        std::strncpy(uuid_str, id, 36);
        uuid_str[36] = '\0';
    }

    // Getters to access client ID and operations
    const char* getId() const{ return uuid_str; }
    int32_t getClientId() const { return client_id; }
    const std::vector<Operation>& getOperations() const { return operations; }
};

#endif