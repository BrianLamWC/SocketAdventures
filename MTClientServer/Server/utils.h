#ifndef UTILS_H
#define UTILS_H

#include <vector>
#include <string>
#include <unordered_map>
#include <cstdint>

#include "partialSequencer.h"
#include "merger.h"
#include "transaction.h"
#include "../proto/request.pb.h"


#define SERVERLIST "servers.json"
#define MOCKDB "data.json"

struct server
{
    std::string ip;
    int port;
    int32_t id;
    bool isOnline;
};

// LISTENER THREAD

struct ListenerThreadsArgs
{
    int listenfd;
    PartialSequencer* partial_sequencer;
    Merger* merger;
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

    Listener(connectionType connection_type, int listenfd, PartialSequencer* partial_sequencer, Merger* merger);

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


// MOCK DATABASE

struct DataItem 
{
    std::string val;
    int32_t primaryCopyID;
};

extern std::unordered_map<std::string, DataItem> mockDB;

// SERVER ID

extern int peer_port;
extern int32_t my_id;

extern std::vector<server> servers;


void error(const char *msg);
int setupListenfd(int my_port);
bool setNonBlocking(int listenfd);
void threadError(const char *msg);
int setupConnection(const std::string& ip, int port);
void setupMockDB();
void getServers();
std::vector<Operation> getOperationsFromProtoTransaction(const request::Transaction& txn_proto);
#endif
