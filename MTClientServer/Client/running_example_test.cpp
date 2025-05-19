#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <iostream>
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
    exit(EXIT_FAILURE);
}

// Write exactly n bytes or exit on error
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

// Generate a UUID string
std::string generateUUID() {
    uuid_t uuid;
    char uuid_str[37];  // 36 chars + null
    uuid_generate(uuid);
    uuid_unparse(uuid, uuid_str);
    return std::string(uuid_str);
}

void sendTransaction(int server_port, const std::vector<int>& keys) {
    // 1) build the Request
    request::Request req;
    req.set_recipient(request::Request::BATCHER);
    req.set_client_id(getpid());  // or any static client id

    auto *txn = req.add_transaction();
    txn->set_order(globalTransactionCounter.fetch_add(1));
    txn->set_id(generateUUID());
    txn->set_client_id(getpid());

    // add the specified write ops
    for (int k : keys) {
        auto *op = txn->add_operations();
        op->set_type(request::Operation::WRITE);
        op->set_key(std::to_string(k));
        // set value to something—here just the same as key
        op->set_value("val" + std::to_string(k));
    }

    // serialize
    std::string serialized;
    if (!req.SerializeToString(&serialized)) {
        error("error serializing request");
    }

    uint32_t payload_size = serialized.size();
    uint32_t netlen = htonl(payload_size);

    // 2) open & connect socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) error("opening socket");

    struct hostent *server = gethostbyname(SERVER_ADDRESS);
    if (!server) error("resolving host");

    struct sockaddr_in serv_addr = {};
    serv_addr.sin_family = AF_INET;
    memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
    serv_addr.sin_port = htons(server_port);

    if (connect(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
        error("connecting");

    // 3) send length + payload
    if (!writeNBytes(sockfd, &netlen, sizeof(netlen)) ||
        !writeNBytes(sockfd, serialized.data(), payload_size)) {
        error("writing to socket");
    }

    // printf("Sent TX (order=%d) with %d ops to port %d\n",
    //        txn->order(), txn->operations_size(), server_port);

    close(sockfd);
}

int main(int argc, char *argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Define your three transactions: port and list of keys to write
    std::vector<std::pair<int, std::vector<int>>> schedule = {
        {7001, {1, 2}}, 
        {7002, {1, 2}},      
        {7001, {1 }},     
    };

    while (true)
    {
        for (auto &entry : schedule) {
            sendTransaction(entry.first, entry.second);
        }
    }
    

    // // Send first two back-to-back:
    // sendTransaction(7001, {1,2});
    // sendTransaction(7002, {1,2});

    // // Now sleep one second:
    // sleep(1);

    // // Then send the third:
    // sendTransaction(7001, {1});

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
