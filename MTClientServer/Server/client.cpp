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
    auto* ptr = static_cast<ClientArgs*>(client_args);
    ClientArgs local = *ptr;
    free(ptr);

    int connfd = local.connfd;
    sockaddr_in client_addr = local.client_addr;

    // get client's IP address and port
    char *client_ip = inet_ntoa(client_addr.sin_addr);
    int client_port = ntohs(client_addr.sin_port);

    //printf("Client connected: %s:%d\n", client_ip, client_port);

    // 1) Read 4-byte length
    uint32_t netlen;
    if (readNBytes(connfd, &netlen, sizeof(netlen)) != sizeof(netlen)) {
        fprintf(stderr, "Failed to read length from %s:%d\n",
                client_ip, client_port);
        close(connfd);
        return nullptr;
    }
    uint32_t msglen = ntohl(netlen);

    // 2) Read exactly msglen bytes
    std::vector<char> buf(msglen);
    ssize_t bytes_read = readNBytes(connfd, buf.data(), msglen);

    if (bytes_read != static_cast<ssize_t>(msglen)) {
        fprintf(stderr,
                "CLIENT_HANDLER: Truncated read: expected %u bytes, got %zd bytes from %s:%d\n",
                msglen,             // what we expected
                bytes_read,         // what we actually got
                client_ip,          // peer IP
                client_port         // peer port
        );
        close(connfd);
        return nullptr;
    }


    // 3) Parse
    request::Request req_proto;
    if (!req_proto.ParseFromArray(buf.data(), msglen)) {
        fprintf(stderr, "ParseFromArray failed (%u bytes) from %s:%d\n",
                msglen,
                client_ip, client_port);
        close(connfd);
        return nullptr;
    }

    // Check if request is for batcher (not done)

    if (req_proto.recipient() != request::Request::BATCHER)
    {
        printf("Invalid request %s:%d \n", client_ip, client_port);
        close(connfd);
        pthread_exit(NULL);
    }
    
    // expecting one transaction per client
    request_queue_.push(req_proto);

    close(connfd);
    pthread_exit(NULL);
}