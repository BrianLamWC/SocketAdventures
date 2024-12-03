#include "server.h"
#include "utils.h"
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include "../proto/request.pb.h"

Queue_TS requestQueue;
Queue_TS partialSequence;
std::vector<server> servers;
int my_port;
std::unordered_map<std::string, DataItem> mockDB;
std::string my_id;

void* handleServer(void *server_args)
{
    ServerArgs* my_args = (ServerArgs*) server_args;
    int connfd = my_args->connfd;
    sockaddr_in server_addr = my_args->server_addr;

    free(server_args);

    // get server's IP address and port
    char *server_ip = inet_ntoa(server_addr.sin_addr);
    int server_port = ntohs(server_addr.sin_port);

    // set up buffer
    char buffer[256];
    int buffer_size = sizeof(buffer);
    int received_bytes;
    
    // clear buffer 
    memset(buffer, 0, buffer_size);

    // read the message from the server
    received_bytes = read(connfd, buffer, 255); // leave one byte for null terminator

    if (received_bytes < 0)
    {
        threadError("error reading from socket"); 
    }

    if (received_bytes == 0)
    {
        printf("Server %s:%d closed their connection\n", server_ip, server_port);
        pthread_exit(NULL);
    }

    // Deserialize the protobuf message from the buffer
    request::Request req_proto;

    if (!req_proto.ParseFromArray(buffer, received_bytes))
    {
        printf("Failed to parse request from server %s:%d\n", server_ip, server_port);
        close(connfd);
        pthread_exit(NULL);
    }

    // Check the recipient
    if (req_proto.recipient() == request::Request::PING)
    {
        //printf("pinged by: %d\n", req_proto.server_id());
        close(connfd);
        pthread_exit(NULL);

    }else if (req_proto.recipient() == request::Request::PARTIAL)
    {
        printf("received transaction from: %d\n", req_proto.server_id());
        close(connfd);
        pthread_exit(NULL);

    }else if (req_proto.recipient() == request::Request::MERGER)
    {
        /* code */
    }else{

    }
    
    
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

        pthread_t server_thread;

        if (pthread_create(&server_thread, NULL, handleServer, (void*)server_args) != 0)
        {
            free(server_args);
            close(connfd);
            error("serverListener: error creating thread");
        }

        pthread_detach(server_thread);
        
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

    int connfd = setupConnection(ip, port);

    if (connfd < 0)
    {
        return false;
    }

    // create a Request message
    request::Request request;
    request.set_server_id(atoi(my_id.c_str()));

    // Set the recipient
    request.set_recipient(request::Request::PING);

    // Create the empty Transaction 
    request::Transaction *transaction = request.mutable_transaction();

    // Serialize the Request message
    std::string serialized_request;
    if (!request.SerializeToString(&serialized_request))
    {
        error("error serializing request");
    }

    // Send serialized request
    int sent_bytes = write(connfd, serialized_request.c_str(), serialized_request.size());
    if (sent_bytes < 0)
    {
        error("error writing to socket");
    }

    close(connfd);

    return true;
}

void Queue_TS::push(const Transaction& val){

    std::lock_guard<std::mutex> lock(mtx);
    q.push(val);

}

std::vector<Transaction> Queue_TS::popAll() {

    std::lock_guard<std::mutex> lock(mtx);
    std::vector<Transaction> result;

    while (!q.empty()) {
        result.push_back(q.front());
        q.pop();
    }

    return result; 
}