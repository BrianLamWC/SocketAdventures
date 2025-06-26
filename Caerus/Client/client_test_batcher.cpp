#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <iostream>
#include <pthread.h>
#include <string>
#include <atomic>
#include <uuid/uuid.h>   // Include libuuid for generating UUIDs
#include "../proto/request.pb.h"

#define SERVER_ADDRESS "localhost"

// Global atomic counter for transaction orders
std::atomic<int32_t> globalTransactionCounter{0};

void error(const char *msg)
{
    perror(msg);
    pthread_exit(NULL);
}

// Write exactly n bytes or return false on error
bool writeNBytes(int fd, const void *buf, size_t n) {
    const char *p = static_cast<const char*>(buf);
    size_t left = n;
    while (left) {
        ssize_t w = ::write(fd, p, left);
        if (w <= 0) return false;
        left -= w;
        p    += w;
    }
    return true;
}

// Function to generate a UUID as a string
std::string generateUUID() {
    uuid_t uuid;
    char uuid_str[37];  // 36 characters plus null terminator
    uuid_generate(uuid);
    uuid_unparse(uuid, uuid_str);
    return std::string(uuid_str);
}

void *clientThread(void *args)
{
    int server_port = *((int *)args);
    int client_id   = static_cast<int>(pthread_self());

    while (true)
    {
        int sockfd;
        struct sockaddr_in serv_addr = {};
        struct hostent *server;

        // Create socket
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) error("error opening socket");

        // Resolve server address
        server = gethostbyname(SERVER_ADDRESS);
        if (server == NULL) error("error resolving server address");

        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr,
              (char *)&serv_addr.sin_addr.s_addr,
              server->h_length);
        serv_addr.sin_port = htons(server_port);

        // Connect to server
        if (connect(sockfd, (struct sockaddr *)&serv_addr,
                    sizeof(serv_addr)) < 0)
        {
            error("error connecting");
        }

        // Build the Request
        request::Request request;
        request.set_recipient(request::Request::BATCHER);
        request.set_client_id(client_id);

        auto *txn = request.add_transaction();
        txn->set_order(globalTransactionCounter.fetch_add(1));
        std::string uuid = generateUUID();
        txn->set_id(uuid);
        txn->set_client_id(client_id);

        // Add some write operations
        for (int i = 1; i <= 3; ++i) {
            auto *op = txn->add_operations();
            op->set_type(request::Operation::WRITE);
            op->set_key(std::to_string(i));
            op->set_value(std::to_string(i+1));
        }

        // Serialize
        std::string serialized;
        if (!request.SerializeToString(&serialized)) {
            error("error serializing request");
        }

        uint32_t payload_size = serialized.size();
        uint32_t netlen = htonl(payload_size);
        
        // 1) send the 4-byte length prefix
        if (!writeNBytes(sockfd, &netlen, sizeof(netlen))) {
            error("error writing length prefix");
        }
        
        // 2) send the actual payload
        if (!writeNBytes(sockfd,
                         serialized.data(),
                         payload_size)) {
            error("error writing payload");
        }
        
        // now the receiver will read exactly netlen first, then payload_size bytes.
        printf("%d sent framed request: %u bytes payload (+4B header). UUID %s\n",
               client_id, payload_size, uuid.c_str());

        close(sockfd);
        sleep(2);
    }

    pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <server port>\n";
        return 1;
    }

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    int server_port = std::stoi(argv[1]);
    pthread_t thread;

    if (pthread_create(&thread, NULL, clientThread, &server_port) != 0) {
        perror("error creating thread");
        return 1;
    }
    pthread_join(thread, NULL);

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
