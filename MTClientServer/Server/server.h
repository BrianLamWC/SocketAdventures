#ifndef SERVER_H
#define SERVER_H

#include <string>
#include <vector>
#include <netinet/in.h>
#include <queue>
#include <condition_variable>
#include <mutex>

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
    std::string data;          // type of transaction: "READ" or "WRITE"        
};

struct Request {
    int client_id;
    Transaction transaction;
};

template <typename T>
class Queue_TS
{
private:

    std::queue<T> q;
    std::condition_variable cv;
    std::mutex mtx;

public:

    void push(T const& val);
    std::vector<T> popAll();
};

extern Queue_TS<Request> requestQueue;

void* serverListener(void *args);
void* pingServers(void *args);
bool pingAServer(const std::string &ip, int port);

#endif // SERVER_H