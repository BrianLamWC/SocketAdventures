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

struct Transaction {
    std::string data;              
};

struct Request {
    int client_id;
    Transaction transaction;
};

class Queue_TS
{
private:

    std::queue<Request> q;
    std::condition_variable cv;
    std::mutex mtx;

public:

    void push(const Request& val);
    std::vector<Request> popAll();
};

struct DataItem
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