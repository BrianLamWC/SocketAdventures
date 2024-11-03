#include "client.h"
#include "utils.h"
#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>

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
                error("clientListener: error accepting connection");
            }
            
        }
        
        // have to malloc to dynamically allocate a seperate ClientArgs struct for each thread
        ClientArgs* client_args = (ClientArgs*) malloc(sizeof(ClientArgs));

        if (!client_args)
        {
            close(connfd);
            error("clientListener: memory allocation failed");
        }

        client_args->connfd = connfd;
        client_args->client_addr = client_addr;

        pthread_t client_thread;

        if (pthread_create(&client_thread, NULL, handleClient, (void*)client_args) != 0)
        {
            free(client_args);  // Free memory on thread creation failure
            close(connfd);
            error("clientListener: error creating thread");
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
    int received_bytes, sent_bytes;

    while (1)
    {
        // clear buffer 
        memset(buffer, 0, buffer_size);

        // read the message from the client
        received_bytes = read(connfd, buffer, 255); // leave one byte for null terminator

        if (received_bytes < 0)
        {
            error("error reading from socket"); 
        }

        if (received_bytes == 0)
        {
            printf("Client %s:%d closed their connection\n", client_ip, client_port);
            break;
        }

        // print received message
        printf("Client %s:%d: %s\n", client_ip, client_port, buffer);

        // send the received message back to the client (echo)
        sent_bytes = write(connfd, buffer, strlen(buffer));
        if (sent_bytes < 0)
        {
            error("error writing to socket");
        }
    }

    close(connfd);
    pthread_exit(NULL);
}