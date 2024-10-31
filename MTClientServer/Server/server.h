#ifndef SERVER_H
#define SERVER_H

#include <string>
#include <vector>
#include <netinet/in.h>

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

// Function prototypes
void* serverListener(void *args);
void* pingServers(void *args);
bool pingAServer(const std::string &ip, int port);

#endif // SERVER_H