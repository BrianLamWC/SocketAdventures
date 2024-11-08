#ifndef SERVER_H
#define SERVER_H

#include <string>
#include <vector>
#include <netinet/in.h>
#include <queue>
#include <condition_variable>
#include <mutex>

extern int my_port;

struct server
{
    std::string ip;
    int port;
    bool isOnline;
};


extern std::vector<server> servers;

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
    std::string data;          // type of transaction: "READ" or "WRITE"        
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

extern Queue_TS requestQueue;

void* serverListener(void *args);
void* pingServers(void *args);
bool pingAServer(const std::string &ip, int port);

#endif // SERVER_H