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
#include "../Server/json.hpp"

#include "../proto/request.pb.h"
#include "../proto/graph_snapshot.pb.h"

using json = nlohmann::json;

std::atomic<int32_t> globalTransactionCounter{1};
std::mt19937 rng{std::random_device{}()};

struct TxnSpec
{
    int target_id;
    request::Operation::OperationType type;
    std::vector<int> keys;
};

std::map<std::string, int> hostnames_to_id = {
    {"192.168.8.140", 1},
    {"192.168.8.150", 2},
    {"192.168.8.160", 3},
};
int target_port = 7001;

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

bool readNBytes(int fd, void *buf, size_t n)
{
    char *p = static_cast<char *>(buf);
    size_t left = n;
    while (left)
    {
        ssize_t r = ::recv(fd, p, left, 0);
        if (r <= 0)
            return false;
        left -= r;
        p += r;
    }
    return true;
}

template <typename Msg>
bool recvProtoFramed(int fd, Msg &msg)
{
    uint32_t len_n;
    if (!readNBytes(fd, &len_n, sizeof(len_n)))
        return false;
    uint32_t len = ntohl(len_n);
    if (len == 0)
        return false;
    std::string buf;
    buf.resize(len);
    if (!readNBytes(fd, &buf[0], len))
        return false;
    return msg.ParseFromString(buf);
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

std::vector<std::vector<TxnSpec>> parseJsonFile(const std::string &filename)
{
    std::ifstream file(filename);
    if (!file.is_open())
    {
        throw std::runtime_error("Could not open file: " + filename);
    }

    nlohmann::json json;
    file >> json;

    std::vector<std::vector<TxnSpec>> batches;
    for (const auto &batch : json)
    {
        std::vector<TxnSpec> specs;
        for (const auto &item : batch)
        {
            TxnSpec spec;
            spec.target_id = item["target_id"];
            spec.type = item["type"] == "WRITE" ? request::Operation::WRITE : request::Operation::READ;
            spec.keys = item["keys"].get<std::vector<int>>();
            specs.push_back(spec);
        }
        batches.push_back(specs);
    }
    return batches;
}

// forward declaration for snapshot helper
void requestSnapshotFromHost(const std::string &host);

void handleCommand(const std::string &command)
{
    // snap with no args -> request from all known servers
    if (command == "snap")
    {
        for (const auto &p : hostnames_to_id)
        {
            requestSnapshotFromHost(p.first);
        }
        return;
    }

    // snap <host> -> request from the given host
    if (command.rfind("snap ", 0) == 0)
    {
        std::string host = command.substr(5);
        requestSnapshotFromHost(host);
        return;
    }
    if (command.rfind("send ", 0) == 0)
    {
        std::string filename = command.substr(5);
        std::vector<std::vector<TxnSpec>> batches;
        try
        {
            batches = parseJsonFile(filename);
        }
        catch (const std::exception &e)
        {
            std::cerr << "Error: " << e.what() << "\n";
            return;
        }

        // Establish connections to all servers first
        std::map<int, int> server_id_to_fd;
        for (const auto &pair : hostnames_to_id)
        {
            int fd = setupConnection(pair.first.c_str(), target_port);
            if (fd < 0)
            {
                std::cerr << "Can't connect to " << pair.first << ":" << target_port << "\n";
            }
            server_id_to_fd[pair.second] = fd;
        }

        // Send each batch in order
        for (size_t batch_idx = 0; batch_idx < batches.size(); ++batch_idx)
        {
            std::cout << "Sending batch " << batch_idx + 1 << "...\n";
            std::vector<request::Request> requests;
            for (const auto &spec : batches[batch_idx])
            {
                requests.push_back(createRequest(spec));
            }
            for (const auto &req : requests)
            {
                int sid = req.target_server_id();
                auto it = server_id_to_fd.find(sid);
                if (it == server_id_to_fd.end() || it->second < 0)
                {
                    std::cerr << "No valid connection for server_id " << sid << "\n";
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
        }

        // Close all connections
        for (auto &kv : server_id_to_fd)
        {
            if (kv.second >= 0)
                close(kv.second);
        }
    }
    else
    {
        std::cerr << "Unknown command: " << command << "\n";
    }
}

// send synchronous GRAPH_SNAP request to a single host and print parsed snapshot
void requestSnapshotFromHost(const std::string &host)
{
    int fd = setupConnection(host.c_str(), target_port);
    if (fd < 0)
    {
        std::cerr << "Can't connect to " << host << ":" << target_port << "\n";
        return;
    }

    request::Request snap_req;
    snap_req.set_client_id(getpid());
    snap_req.set_recipient(request::Request::GRAPH_SNAP);

    if (!sendProtoFramed(fd, snap_req))
    {
        std::cerr << "Failed to send GRAPH_SNAP to " << host << "\n";
        close(fd);
        return;
    }

    request::GraphSnapshot snap;
    if (!recvProtoFramed(fd, snap))
    {
        std::cerr << "Failed to receive GraphSnapshot from " << host << "\n";
        close(fd);
        return;
    }

    std::cout << "GraphSnapshot from " << host << ": node_id=" << snap.node_id() << "\n";
    for (int i = 0; i < snap.adj_size(); ++i)
    {
        const auto &va = snap.adj(i);
        std::cout << "  tx=" << va.tx_id() << " ->";
        for (int j = 0; j < va.out_size(); ++j)
            std::cout << " " << va.out(j);
        std::cout << "\n";
    }

    close(fd);
}

int main()
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    std::string command;
    while (true)
    {
        std::cout << "Enter command: ";
        std::getline(std::cin, command);
        if (command == "exit")
        {
            break;
        }
        handleCommand(command);
    }
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
