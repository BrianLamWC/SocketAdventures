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
#include <unistd.h>
#include <vector>
#include <thread>
#include <fstream>
#include <random>

#include "../proto/request.pb.h"

std::atomic<int32_t> globalTransactionCounter{1};
std::mt19937 rng{std::random_device{}()};

struct TxnSpec
{
    int target_id;
    request::Operation::OperationType type;
    std::vector<int> keys;
};

int setupConnection(const char *host, int port)
{
    struct addrinfo hints{}, *res;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    char portstr[6];
    snprintf(portstr, sizeof(portstr), "%d", port);
    if (getaddrinfo(host, portstr, &hints, &res))
        return -1;
    int fd = -1;
    for (auto p = res; p; p = p->ai_next)
    {
        fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0)
            continue;
        if (connect(fd, p->ai_addr, p->ai_addrlen) == 0)
            break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

bool writeNBytes(int fd, const void *buf, size_t n)
{
    const char *p = static_cast<const char *>(buf);
    size_t left = n;
    while (left)
    {
        ssize_t w = ::send(fd, p, left, MSG_NOSIGNAL);
        if (w <= 0)
            return false;
        left -= w;
        p += w;
    }
    return true;
}

request::Request createRequest(const TxnSpec &spec)
{
    request::Request req;
    req.set_recipient(request::Request::BATCHER);
    req.set_client_id(getpid());
    req.set_target_server_id(spec.target_id);

    auto *t = req.add_transaction();
    t->set_id(std::to_string(globalTransactionCounter.fetch_add(1)));
    t->set_client_id(getpid());

    for (int k : spec.keys)
    {
        auto *op = t->add_operations();
        op->set_type(spec.type);
        op->set_key(std::to_string(k));
        if (spec.type == request::Operation::WRITE)
        {
            std::uniform_int_distribution<int> value_dist(1000, 9999);
            op->set_value(std::to_string(value_dist(rng)));
        }
    }

    return req;
}

bool sendProtoFramed(int fd, const google::protobuf::Message &msg)
{
    std::string bytes;
    if (!msg.SerializeToString(&bytes))
        return false;

    uint32_t n = static_cast<uint32_t>(bytes.size());
    uint32_t be = htonl(n); // 4-byte big-endian length prefix
    if (!writeNBytes(fd, &be, sizeof(be)))
        return false;
    if (!writeNBytes(fd, bytes.data(), bytes.size()))
        return false;
    return true;
}

int main()
{

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // setup connections to batchers
    std::map<std::string, int> hostnames_to_id = {
        {"192.168.8.140", 1},
        {"192.168.8.150", 2},
        {"192.168.8.160", 3},
    };

    int target_port = 7001;

    std::unordered_map<int, int> server_id_to_fd;

    for (const auto &pair : hostnames_to_id)
    {
        int fd = setupConnection(pair.first.c_str(), target_port);
        if (fd < 0)
        {
            fprintf(stderr, "Can't connect to %s:%d\n", pair.first.c_str(), target_port);
            exit(1);
        }

        server_id_to_fd[pair.second] = fd;
    }

    // intermediate struct to hold info the transactions before converting to protobuf struct
    std::vector<std::vector<TxnSpec>> batches = {
        {
            {1, request::Operation::WRITE, {1, 2}},   // T1: W1,W2
            {2, request::Operation::WRITE, {2}},      // T2:W1,W2
            {1, request::Operation::WRITE, {1, 2, 3}} // T3:W1
        },
        {
            {1, request::Operation::WRITE, {1, 2}}, // T1:W1,W2
            {2, request::Operation::WRITE, {1, 2}}, // T2:W3
            {1, request::Operation::WRITE, {1}}     // T3:W1,W2,W3

        },
        {
            {1, request::Operation::WRITE, {1}}, // T1: W1,W2
            {1, request::Operation::READ, {1}},  // T2:W1,W2
            {1, request::Operation::READ, {1}},  // T3:W1
            {1, request::Operation::WRITE, {1}}  // T4:W3

        }};

    // struct to hold batches of transactions in protobuf format
    std::vector<std::vector<request::Request>> batches_pb;
    for (const auto &batch : batches)
    {
        std::vector<request::Request> pb_vec;
        for (const auto &spec : batch)
        {
            pb_vec.push_back(createRequest(spec));
        }
        batches_pb.push_back(std::move(pb_vec));
    }

    // send transactions in batch, wait for a second between batches

    for (size_t i = 0; i < batches_pb.size(); ++i)
    {
        const auto &batch = batches_pb[i];
        std::cout << "Sending batch " << i << " (" << batch.size() << " txns)\n";

        for (const auto &req : batch)
        {
            // we set target_server_id() in createRequest(...)
            int sid = req.target_server_id();
            auto it = server_id_to_fd.find(sid);
            if (it == server_id_to_fd.end())
            {
                std::cerr << "No fd for server_id " << sid << "\n";
                continue;
            }
            int fd = it->second;

            if (!sendProtoFramed(fd, req))
            {
                std::cerr << "Send failed to server_id " << sid << "\n";
            }
            else
            {
                std::cout << "  Sent txn " << req.transaction(0).id() << " to server " << sid << "\n";
            }
        }

        std::this_thread::sleep_for(std::chrono::seconds(1)); // gap between batches
    }

    for (auto &kv : server_id_to_fd)
        close(kv.second);
    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}
