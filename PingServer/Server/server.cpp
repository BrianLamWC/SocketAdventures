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
#include <sys/mman.h>
#include <signal.h>
#include <sys/wait.h>

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

std::vector<server> getServers()
{

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

bool pingAServer(const std::string &ip, int port)
{

    int connfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in server_addr = {};
    struct hostent *server;

    if (connfd < 0)
    {
        error("pingAServer: error opening socket");
    }

    server = gethostbyname(ip.c_str());

    if (server == NULL)
    {
        error("pingAServer: server does not exist");
    }

    // memset((void *) &server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;

    memcpy((void *)&server_addr.sin_addr.s_addr, server->h_addr, server->h_length);

    server_addr.sin_port = htons(port);

    if (connect(connfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        close(connfd);
        return false;
    }

    close(connfd);

    return true;
}

void pingServers(struct server *servers, int num_servers, int my_port)
{

    for (int i = 0; i < num_servers; ++i)
    {
        if (servers[i].port == my_port)
        {
            continue;
        }

        servers[i].isOnline = pingAServer(servers[i].ip, servers[i].port);

        printf("server %s:%d status: %d\n", servers[i].ip.c_str(), servers[i].port, servers[i].isOnline);
    }
}

void sigchld_handler(int s)
{

    // Wait for all child processes that have terminated
    // WNOHANG: return if no terminated child processes instead of blocking
    while (waitpid(-1, NULL, WNOHANG) > 0)
        ;
}

void handleChildProcesses()
{

    struct sigaction sa = {};
    sa.sa_handler = sigchld_handler;
    sa.sa_flags = SA_RESTART; // Restart interrupted system calls
    sigaction(SIGCHLD, &sa, NULL);
}

int setupListenfd(int my_port)
{

    int listenfd;
    struct sockaddr_in my_addr;

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
    if (bind(listenfd, (struct sockaddr *)&my_addr, sizeof(my_addr)) < 0)
    {
        error("error binding my address to listening socket");
    }

    // set listening socket to non blocking
    if (!setNonBlocking(listenfd))
    {
        error("error setting socket to non-blocking mode");
    }

    return listenfd;
}

int main(int argc, char *argv[])
{

    if (argc != 2)
    {
        std::cerr << "Usage: " << argv[0] << " <port for servers>\n";
        return 1;
    }

    int my_port = std::stoi(argv[1]);

    // set up signal handler for child processes
    handleChildProcesses();

    // get list of servers
    std::vector<server> servers_vec = getServers();
    int num_servers = servers_vec.size();

    // Create shared memory with mmap
    struct server *servers = (struct server *)mmap(NULL, num_servers * sizeof(server), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    if (servers == MAP_FAILED)
    {
        perror("mmap failed");
        exit(1);
    }

    // copy list of servers into shared memory
    mempcpy((void *)servers, servers_vec.data(), num_servers * sizeof(server));

    // set up server's listening sockets
    int my_listenfd, connfd;

    my_listenfd = setupListenfd(my_port);

    // listening for incoming connections
    listen(my_listenfd, 5);

    printf("Listening for servers on port %d\n", my_port);

    // fork a child to ping servers periodically
    pid_t pid_ping = fork();

    if (pid_ping < 0)
    {
        error("error forking");
    }
    else if (pid_ping == 0)
    {

        // Child process to handle pinging servers

        while (1)
        {
            system("clear");
            pingServers(servers, num_servers, my_port);
            sleep(5);
        }

        exit(0);
    }

    struct sockaddr_in client_addr;
    socklen_t client_addrlen = sizeof(client_addr);

    while (1)
    {

        // accept a client
        connfd = accept(my_listenfd, (struct sockaddr *)&client_addr, &client_addrlen);

        if (connfd < 0)
        {
            if (errno == EWOULDBLOCK) // no clients
            {
                usleep(100000); // cpu-friendly
                continue;
            }
            else
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

            // listening sockets not needed anymore
            close(my_listenfd);
            exit(0); // exit child process when done
        }
        else
        {
            // parent process continues to accept new clients
            close(connfd); // connection socket not needed anymore in parent process
        }
    }

    close(my_listenfd);

    return 0;
}