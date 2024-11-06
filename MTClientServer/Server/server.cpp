#include "server.h"
#include "utils.h"
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>

void* handleServer(void *server_args)
{
    ServerArgs* my_args = (ServerArgs*) server_args;
    int connfd = my_args->connfd;
    sockaddr_in server_addr = my_args->server_addr;

    free(server_args);

    // get server's IP address and port
    char *server_ip = inet_ntoa(server_addr.sin_addr);
    int server_port = ntohs(server_addr.sin_port);

    printf("pinged by %s:%d\n", server_ip, server_port);

    close(connfd);
    pthread_exit(NULL);
}

void* serverListener(void *args)
{
    ListenerThreadsArgs *my_args = (ListenerThreadsArgs*) args;
    struct sockaddr_in server_addr;
    socklen_t server_addrlen = sizeof(server_addr);

    while (true)
    {
        int connfd = accept(my_args->listenfd, (struct sockaddr*)&server_addr, &server_addrlen);

        if (connfd < 0)
        {
            if (errno == EWOULDBLOCK)
            {
                usleep(10000);
                continue;
            } else
            {
                error("serverListener: error accepting connection");
            }
            
        }
        
        // server args
        ServerArgs* server_args = (ServerArgs*)malloc(sizeof(ServerArgs));

        if (!server_args)
        {
            close(connfd);
            error("serverListener: memory allocation failed");
        }

        server_args->connfd = connfd;
        server_args->server_addr = server_addr;

        pthread_t client_thread;

        if (pthread_create(&client_thread, NULL, handleServer, (void*)server_args) != 0)
        {
            free(server_args);
            close(connfd);
            error("serverListener: error creating thread");
        }

        pthread_detach(client_thread);
        
    }
    
    pthread_exit(NULL);
}

void* pingServers(void *args)
{
    PingerThreadArgs* my_args = (PingerThreadArgs*) args;
    std::vector<server>* servers = my_args->servers;

    while (1)
    {

        // system("clear");

        for (auto& server : *servers)
        {
            if (server.port == my_args->my_port)
            {
                continue;
            }

            server.isOnline = pingAServer(server.ip, server.port);
            // printf("server %s:%d status: %d\n", server.ip.c_str(), server.port, server.isOnline);
        }

        sleep(5);
    }
    
    pthread_exit(NULL);

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

template <typename T>
void Queue_TS<T>::push(T const& val){

    std::lock_guard<std::mutex> lock(mtx);
    q.push(val);

}

template <typename T>
std::vector<T> Queue_TS<T>::popAll() {

    std::lock_guard<std::mutex> lock(mtx);
    std::vector<T> result;

    while (!q.empty()) {
        result.push_back(q.front());
        q.pop();
    }

    return result; 
}