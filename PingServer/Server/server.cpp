#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include "json.hpp"
#include <sys/time.h>
#include <netdb.h>
#include <cstdlib>

using json = nlohmann::json;

#define SERVERLIST "servers.json"

void error(const char *msg)
{
    perror(msg);
    exit(1);
}

struct server
{
    std::string ip;
    int port;
    bool isOnline;
};

std::vector<server> getServers(){
    std::ifstream file(SERVERLIST);

    if (!file.is_open())
    {
        error("getServers: error opening file");
        exit(1);
    }
    
    json data = json::parse(file);

    std::vector<server> result;

    auto servers = data["servers"];

    for (auto server : servers)
    {
        result.push_back({server["ip"], server["port"], false});
    }
    
    file.close();

    return result;
}

bool setNonBlocking(int listenfd){
    if (listenfd < 0)
    {
        error("setNonBlocking: error setting non blocking, invalid listening socket");
        return false;
    }
    
    int flags  = fcntl(listenfd, F_GETFL, 0);

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

bool pingAServer(const std::string& ip, int port){
    
    int connfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in server_addr;
    struct hostent *server;

    if (connfd < 0)
    {
        error("pingAServer: error opening socket");
    }

    server = gethostbyname(ip.c_str());

    if (server == NULL)
    {
        error("pingAServer: error no such host");
    } 
    
    memset((void *) &server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;

    memcpy((void *) &server_addr.sin_addr.s_addr, server->h_addr, server->h_length);

    server_addr.sin_port = htons(port);

    if (connect(connfd, (struct sockaddr *) &server_addr, sizeof(server_addr)) < 0) {
        close(connfd);
        return false;
    }

    close(connfd);

    return true;
}

int main(int argc, char *argv[]){

    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <port>\n";
        return 1;
    }

    int my_port = std::stoi(argv[1]);

    std::vector<server> servers = getServers();

    // set up server's listening socket
    int listenfd, connfd;
    struct sockaddr_in my_addr, client_addr;

    socklen_t client_addrlen = sizeof(client_addr);

    // create listening socket
    listenfd = socket(AF_INET, SOCK_STREAM, 0);

    if (listenfd < 0)
    {
        error("error creating listening socket");
    }

    // configure server address structure
    my_addr.sin_family = AF_INET;
    my_addr.sin_addr.s_addr = INADDR_ANY;
    my_addr.sin_port = htons(my_port);

    // bind listening socket to server address and port
    if (bind(listenfd, (struct sockaddr *) &my_addr, sizeof(my_addr)) < 0)
    {
        error("error binding my address to listening socket");
    }
    
    // set listening socket to non blocking
    if (!setNonBlocking(listenfd))
    {
        error("error setting socket to non-blocking mode");
    }
    
    // listening for incoming connections
    listen(listenfd, 5);

    printf("Listening on port %d\n", ntohs(my_addr.sin_port));

    struct timeval last_ping_time, current_time;
    gettimeofday(&last_ping_time, NULL);

    while (1)
    {

        // Check time passed
        gettimeofday(&current_time, NULL);
        long seconds_passed = current_time.tv_sec - last_ping_time.tv_sec;

        // ping other servers every 10 seconds
        if (seconds_passed >= 5)
        {
            system("clear");

            for (auto &server : servers)
            {
                if (server.port == my_port)
                {
                    continue;
                }
                
                server.isOnline = pingAServer(server.ip, server.port);

                printf("server %s:%d status: %d\n", server.ip.c_str(), server.port, server.isOnline);

            }
        
            last_ping_time = current_time;
        }

        // accept a client
        connfd = accept(listenfd, (struct sockaddr *)&client_addr, &client_addrlen);
        
        if (connfd < 0)
        {
            if (errno == EWOULDBLOCK)
            {
                usleep(100000);
                continue;
            }else
            {
                error("error accepting a connection");
            }
            
        }

        // fork a new process to handle the client
        pid_t pid = fork();

        if (pid < 0)
        {
            error("error forking");
        }
        else if (pid == 0)
        {
            // child process handles the client
            close(listenfd); // listening socket not needed anymore
            exit(0); // exit child process when done
        }
        else
        {
            // parent process continues to accept new clients
            close(connfd); // connection socket not needed anymore in parent process
        }
    }

    close(listenfd);

    return 0;
}