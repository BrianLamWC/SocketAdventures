#ifndef UTILS_H
#define UTILS_H

#include <vector>
#include <string>
#include <unordered_map>
#include <queue>
#include <mutex>

#define SERVERLIST "servers.json"
#define MOCKDB "data.json"

void error(const char *msg);
int setupListenfd(int my_port);
bool setNonBlocking(int listenfd);
void threadError(const char *msg);
int setupConnection(const std::string& ip, int port);
void setupMockDB();
void getServers();
std::vector<Operation> getOperationsFromProto(const request::Request& req_proto);

struct server
{
    std::string ip;
    int port;
    std::string id;
    bool isOnline;
};

// LISTENER THREAD

struct ListenerThreadsArgs
{
    int listenfd;
};

enum class connectionType{
    CLIENT,
    PEER
};

class Listener
{
private:
    ListenerThreadsArgs args;
public:

    Listener(connectionType connection_type, int listenfd);

};

// PINGER THREAD

struct PingerThreadArgs
{
    std::vector<server>* servers;
    int num_servers;
    int my_port;
};

class Pinger
{
private:
    PingerThreadArgs args;
public:
    Pinger(std::vector<server>* servers, int num_servers, int my_port);
    void* pingPeers();
    bool pingAPeer(const std::string &ip, int port);
};


// TRANSACTION

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

// MOCK DATABASE

struct DataItem 
{
    std::string val;
    std::string primaryCopyID;
};

extern std::unordered_map<std::string, DataItem> mockDB;

// SERVER IDENTIFICATION

extern int peer_port;
extern std::string my_id;

extern std::vector<server> servers;

// QUEUES

class Queue_TS
{
private:

    std::queue<Transaction> q;
    std::mutex mtx;

public:

    void push(const Transaction& val);
    std::vector<Transaction> popAll();

};

extern Queue_TS requestQueue;
extern Queue_TS partialSequence;

#endif
