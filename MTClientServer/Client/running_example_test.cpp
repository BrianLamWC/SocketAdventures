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
#include <uuid/uuid.h>
#include "../proto/request.pb.h"

#define SERVER_ADDRESS "localhost"

// Global atomic counter for transaction orders
std::atomic<int32_t> globalTransactionCounter{1};

void error(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

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

std::string generateUUID() {
    uuid_t uuid;
    char uuid_str[37];
    uuid_generate(uuid);
    uuid_unparse(uuid, uuid_str);
    return std::string(uuid_str);
}

// Now take an opType parameter so you can choose READ vs WRITE
void sendTransaction(int server_port,
                     request::Operation::OperationType opType,
                     const std::vector<int>& keys) {
    request::Request req;
    req.set_recipient(request::Request::BATCHER);
    req.set_client_id(getpid());

    auto *txn = req.add_transaction();
    txn->set_id(std::to_string(globalTransactionCounter.fetch_add(1)));
    txn->set_client_id(getpid());

    for (int k : keys) {
        auto *op = txn->add_operations();
        op->set_type(opType);
        op->set_key(std::to_string(k));
        if (opType == request::Operation::WRITE) {
            op->set_value("val" + std::to_string(k));
        }
    }

    std::string serialized;
    if (!req.SerializeToString(&serialized)) {
        error("error serializing request");
    }

    uint32_t payload_size = serialized.size();
    uint32_t netlen = htonl(payload_size);

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

    if (!writeNBytes(sockfd, &netlen, sizeof(netlen)) ||
        !writeNBytes(sockfd, serialized.data(), payload_size)) {
        error("writing to socket");
    }

    close(sockfd);
}

int main(int argc, char *argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // A little struct to hold port, op-type, and keys
    struct TxnSpec {
        int port;
        request::Operation::OperationType type;
        std::vector<int> keys;
    };

    std::vector<TxnSpec> schedule = {
        {7001, request::Operation::WRITE, {1,2}},
        {7003, request::Operation::WRITE, {3}},
        {7003, request::Operation::WRITE, {1,2,3}},
    };

    // while (true)
    // {
    //     for (auto &spec : schedule) {
    //         sendTransaction(spec.port, spec.type, spec.keys);
    //     }
    // }
    
    // for (auto &spec : schedule) {
    //     sendTransaction(spec.port, spec.type, spec.keys);
    // }


    sendTransaction(7001, request::Operation::WRITE, {1,2});
    sendTransaction(7002, request::Operation::WRITE, {1,2});
    sleep(1);
    sendTransaction(7001, request::Operation::WRITE, {1});

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
