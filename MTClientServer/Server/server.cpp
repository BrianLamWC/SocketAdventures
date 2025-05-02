#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>

#include "server.h"
#include "utils.h"
#include "partialSequencer.h"
#include "../proto/request.pb.h"

void *handlePeer(void *server_args)
{
    // 1) Cast and copy the entire struct
    auto* ptr = static_cast<ServerArgs*>(server_args);
    ServerArgs local = *ptr;     // copies connfd, server_addr, partial_sequencer, merger…
    free(ptr);

    // 2) Now use 'local' safely
    int connfd               = local.connfd;
    sockaddr_in server_addr  = local.server_addr;
    auto* partial_sequencer   = local.partial_sequencer;
    auto* merger             = local.merger;

    // get server's IP address and port
    char *server_ip = inet_ntoa(server_addr.sin_addr);
    int server_port = ntohs(server_addr.sin_port);

    // 1) Read 4-byte length
    uint32_t netlen;
    if (readNBytes(connfd, &netlen, sizeof(netlen)) != sizeof(netlen)) {
        fprintf(stderr, "Failed to read length from %s:%d\n",
                server_ip, server_port);
        close(connfd);
        return nullptr;
    }
    uint32_t msglen = ntohl(netlen);

    // 2) Read exactly msglen bytes
    std::vector<char> buf(msglen);
    if (readNBytes(connfd, buf.data(), msglen) != (ssize_t)msglen) {
        fprintf(stderr, "Truncated read (%u bytes) from %s:%d\n",
                msglen,
                server_ip, server_port);
        close(connfd);
        return nullptr;
    }

    // 3) Parse
    request::Request req_proto;
    if (!req_proto.ParseFromArray(buf.data(), msglen)) {
        fprintf(stderr, "ParseFromArray failed (%u bytes) from %s:%d\n",
                msglen,
                server_ip, server_port);
        close(connfd);
        return nullptr;
    }

    // Check the recipient
    if (req_proto.recipient() == request::Request::PING)
    {
        // printf("pinged by: %d\n", req_proto.server_id());
        close(connfd);
        pthread_exit(NULL);
    }
    else if (req_proto.recipient() == request::Request::PARTIAL)
    {
        printf("PARTIAL: received transaction %s from: %d\n", req_proto.transaction(0).id().c_str(), req_proto.server_id());
        partial_sequencer->pushReceivedTransactionIntoPartialSequence(req_proto);
        close(connfd);
        pthread_exit(NULL);
    }
    else if (req_proto.recipient() == request::Request::MERGER)
    {
        printf("MERGER: received partial sequence from: %d\n", req_proto.server_id());
        merger->processProtoRequest(req_proto);
        close(connfd);
        pthread_exit(NULL);
    }
    else
    {
    }

    close(connfd);
    pthread_exit(NULL);
}

void *peerListener(void *args)
{
    ListenerThreadsArgs *my_args = (ListenerThreadsArgs *)args;
    struct sockaddr_in server_addr;
    socklen_t server_addrlen = sizeof(server_addr);

    while (true)
    {
        int connfd = accept(my_args->listenfd, (struct sockaddr *)&server_addr, &server_addrlen);

        if (connfd < 0)
        {
            if (errno == EWOULDBLOCK)
            {
                usleep(10000);
                continue;
            }
            else
            {
                threadError("serverListener: error accepting connection");
            }
        }

        // server args
        ServerArgs *server_args = (ServerArgs *)malloc(sizeof(ServerArgs));

        if (!server_args)
        {
            close(connfd);
            threadError("serverListener: memory allocation failed");
        }

        server_args->connfd = connfd;
        server_args->server_addr = server_addr;
        server_args->partial_sequencer = my_args->partial_sequencer;
        server_args->merger = my_args->merger;

        pthread_t server_thread;

        if (pthread_create(&server_thread, NULL, handlePeer, (void *)server_args) != 0)
        {
            free(server_args);
            close(connfd);
            threadError("serverListener: error creating thread");
        }

        pthread_detach(server_thread);
    }

    pthread_exit(NULL);
}
