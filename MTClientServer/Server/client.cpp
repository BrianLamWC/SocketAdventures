#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>

#include "client.h"
#include "utils.h"
#include "../proto/request.pb.h"

void* clientListener(void *args)
{
    ListenerThreadsArgs *my_args = (ListenerThreadsArgs *) args;
    struct sockaddr_in client_addr;
    socklen_t client_addrlen = sizeof(client_addr);

    while (true)
    {
        int connfd = accept(my_args->listenfd, (struct sockaddr*)&client_addr, &client_addrlen);

        if (connfd < 0)
        {
            if (errno == EWOULDBLOCK)
            {
                usleep(10000);
                continue;
            } else{
                threadError("clientListener: error accepting connection");
            }
            
        }
        
        // have to malloc to dynamically allocate a seperate ClientArgs struct for each thread
        ClientArgs* client_args = (ClientArgs*) malloc(sizeof(ClientArgs));

        if (!client_args)
        {
            close(connfd);
            threadError("clientListener: memory allocation failed");
        }

        client_args->connfd = connfd;
        client_args->client_addr = client_addr;

        pthread_t client_thread;

        if (pthread_create(&client_thread, NULL, handleClient, (void*)client_args) != 0)
        {
            free(client_args);  // Free memory on thread creation failure
            close(connfd);
            threadError("clientListener: error creating thread");
        }

        pthread_detach(client_thread);
        
    }
    
    pthread_exit(NULL);
}

void* handleClient(void *client_args)
{
    ClientArgs* my_args = (ClientArgs *)client_args; //make a local copy
    int connfd = my_args->connfd;
    sockaddr_in client_addr = my_args->client_addr;

    free(client_args);

    // get client's IP address and port
    char *client_ip = inet_ntoa(client_addr.sin_addr);
    int client_port = ntohs(client_addr.sin_port);

    printf("Client connected: %s:%d\n", client_ip, client_port);

    // set up buffer
    char buffer[256];
    int buffer_size = sizeof(buffer);
    int received_bytes;
    
    // clear buffer 
    memset(buffer, 0, buffer_size);

    // read the message from the client
    received_bytes = read(connfd, buffer, 255); // leave one byte for null terminator

    if (received_bytes < 0)
    {
        threadError("error reading from socket"); 
    }

    if (received_bytes == 0)
    {
        printf("Client %s:%d closed their connection\n", client_ip, client_port);
        pthread_exit(NULL);
    }

    // Deserialize the protobuf message from the buffer
    request::Request req_proto;

    if (!req_proto.ParseFromArray(buffer, received_bytes))
    {
        printf("Failed to parse request from client %s:%d\n", client_ip, client_port);
        close(connfd);
        pthread_exit(NULL);
    }

    // Check if request is for batcher (not done)

    if (req_proto.recipient() != request::Request::BATCHER)
    {
        printf("Invalid request %s:%d \n", client_ip, client_port);
        close(connfd);
        pthread_exit(NULL);
    }
    
    // expecting one transaction per client
    std::vector<Operation> operations = getOperationsFromProtoTransaction(req_proto.transaction(0));

    Transaction transaction(req_proto.transaction(0).id().c_str() ,req_proto.client_id(), operations);
    
    request_queue.push(transaction);

    close(connfd);
    pthread_exit(NULL);
}