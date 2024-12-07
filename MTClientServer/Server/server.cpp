#include "server.h"
#include "utils.h"
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include "../proto/request.pb.h"

void* handlePeer(void *server_args)
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

void* peerListener(void *args)
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
                threadError("serverListener: error accepting connection");
            }
            
        }
        
        // server args
        ServerArgs* server_args = (ServerArgs*)malloc(sizeof(ServerArgs));

        if (!server_args)
        {
            close(connfd);
            threadError("serverListener: memory allocation failed");
        }

        server_args->connfd = connfd;
        server_args->server_addr = server_addr;

        pthread_t server_thread;

        if (pthread_create(&server_thread, NULL, handlePeer, (void*)server_args) != 0)
        {
            free(server_args);
            close(connfd);
            threadError("serverListener: error creating thread");
        }

        pthread_detach(server_thread);
        
    }
    
    pthread_exit(NULL);
}


