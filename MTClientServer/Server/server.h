#ifndef SERVER_H
#define SERVER_H

#include <string>
#include <vector>
#include <netinet/in.h>
#include <queue>
#include <condition_variable>
#include <mutex>
#include <unordered_map>

struct server
{
    std::string ip;
    int port;
    std::string id;
    bool isOnline;
};

struct ListenerThreadsArgs
{
    int listenfd;
};

struct PingerThreadArgs
{
    std::vector<server>* servers;
    int num_servers;
    int my_port;
};

struct ServerArgs
{
    int connfd;
    struct sockaddr_in server_addr;
};

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
    int client_id;
    std::vector<Operation> operations;
public:
    // Constructor to initialize a Transaction
    Transaction(int client_id, const std::vector<Operation>& ops)
        : client_id(client_id), operations(ops) {}

    // Getters to access client ID and operations
    int getClientId() const { return client_id; }
    const std::vector<Operation>& getOperations() const { return operations; }
};

class Queue_TS
{
private:

    std::queue<Transaction> q;
    std::mutex mtx;

public:

    void push(const Transaction& val);
    std::vector<Transaction> popAll();

};

struct DataItem // for mock database
{
    std::string val;
    std::string primaryCopyID;
};

extern int my_port;
extern std::vector<server> servers;
extern Queue_TS requestQueue;
extern std::unordered_map<std::string, DataItem> mockDB;

void* serverListener(void *args);
void* pingServers(void *args);
bool pingAServer(const std::string &ip, int port);

#endif // SERVER_H