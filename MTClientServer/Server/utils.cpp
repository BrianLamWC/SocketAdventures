#include "utils.h"
#include <iostream>
#include <fstream>
#include <fcntl.h>
#include "json.hpp"
#include "server.h"
#include <netdb.h>
#include <unistd.h>

using json = nlohmann::json;

void error(const char *msg)
{
    perror(msg);
    exit(1);
}

void threadError(const char *msg)
{
    perror(msg);
    pthread_exit(NULL);  // Terminate only the calling thread
}

int setupConnection(const std::string &ip, int port)
{

    int connfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in server_addr = {};
    struct hostent* server;

    if (connfd < 0) {
        perror("setupConnection: error opening socket");
        return -1;
    }

    server = gethostbyname(ip.c_str());
    if (server == NULL) {
        perror("setupConnection: server does not exist");
        close(connfd);
        return -1;
    }

    server_addr.sin_family = AF_INET;
    memcpy((void*)&server_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    server_addr.sin_port = htons(port);

    if (connect(connfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        // perror("setupConnection: error connecting");
        close(connfd);
        return -1;
    }

    return connfd;  // Return the connected socket descriptor
}

int setupListenfd(int my_port)
{

    int listenfd;
    struct sockaddr_in my_addr;

    // create listening socket
    listenfd = socket(AF_INET, SOCK_STREAM, 0);

    int opt  = 1;

    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        error("setupListenfd: setsockopt failed");
    }

    if (listenfd < 0)
    {
        error("setupListenfd: error creating listening socket");
    }

    // configure server address structure
    my_addr.sin_family = AF_INET;
    my_addr.sin_addr.s_addr = INADDR_ANY;
    my_addr.sin_port = htons(my_port);

    // bind listening socket to server address and port
    if (bind(listenfd, (struct sockaddr *)&my_addr, sizeof(my_addr)) < 0)
    {
        error("setupListenfd: error binding my address to listening socket");
    }

    // set listening socket to non blocking
    if (!setNonBlocking(listenfd))
    {
        error("setupListenfd: error setting socket to non-blocking mode");
    }

    return listenfd;
}

bool setNonBlocking(int listenfd)
{

    if (listenfd < 0)
    {
        error("setNonBlocking: error setting non blocking, invalid listening socket");
        return false;
    }

    int flags = fcntl(listenfd, F_GETFL, 0);

    if (flags == -1)
    {
        error("setNonBlocking: error getting file access mode and file status flags");
        return false;
    }

    if (fcntl(listenfd, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        error("setNonBlocking: error setting file access mode and file status flags");
        return false;
    }

    return true;
}


void setupMockDB(){
    
    std::ifstream file(MOCKDB);

    if (!file.is_open())
    {
        error("getServers: error opening file");
        exit(1);
    }

    json data = json::parse(file);

    auto data_items = data["data_items"];

    for (auto data_item : data_items)
    {
        mockDB.insert({data_item["key"], {data_item["value"], data_item["primaryCopy"]} });
    }
    
    file.close();

};

void getServers()
{

    std::ifstream file(SERVERLIST);

    if (!file.is_open())
    {
        error("getServers: error opening file");
        exit(1);
    }

    json data = json::parse(file);

    auto servers_list = data["servers"];

    for (auto& server : servers_list)
    {
        servers.push_back({server["ip"], server["port"], server["id"], false});
    }

    file.close();

}