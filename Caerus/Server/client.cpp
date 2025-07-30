#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>

#include "client.h"

#include "../proto/request.pb.h"

ClientListener::ClientListener(int listenfd, Logger* logger, Merger* merger)
{

    args =  {listenfd, logger};
    pthread_t listener_thread;

    if (pthread_create(&listener_thread, NULL, clientListener, (void*)&args) != 0)
    {
        threadError("error creating client listener thread");
    }
    
    pthread_detach(listener_thread);

}

void* clientListener(void *args)
{
    ClientListenerThreadsArgs *my_args = (ClientListenerThreadsArgs *) args;
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
        client_args->logger = my_args->logger;

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
    char *client_ip = inet_ntoa(client_addr.sin_addr);
    int client_port = ntohs(client_addr.sin_port);
    Logger* logger = local.logger;
    Merger* merger = local.merger;

    while (true)
    {
        // read the 4-byte length prefix
        uint32_t netlen;
        ssize_t len = readNBytes(connfd, &netlen, sizeof(netlen));
        if (len == 0) {
            // peer cleanly closed
            break;
        }
        if (len < 0 || len != sizeof(netlen)) {
            fprintf(stderr, "CLIENT_HANDLER: length read error from %s:%d\n",
                    client_ip, client_port);
            break;
        }

        uint32_t msglen = ntohl(netlen);

        // read exactly msglen bytes
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
            break;
        }
    
        // parse
        request::Request req_proto;
        if (!req_proto.ParseFromArray(buf.data(), msglen)) {
            fprintf(stderr, "ParseFromArray failed (%u bytes) from %s:%d\n",
                    msglen,
                    client_ip, client_port);
            break;
        }

        if (req_proto.recipient() == request::Request::DUMP) {

            printf("Received DUMP from server %d\n", req_proto.server_id());
            if (logger) {
                printf("Logging DUMP request from client %d\n", req_proto.client_id());
                logger->dumpDB();
            } else {
                fprintf(stderr, "Logger not initialized, cannot log DUMP request\n");
            }



            break;

        }

        request_queue_.push(req_proto);

    }

    close(connfd);
    return nullptr;
}