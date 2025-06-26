#include <iostream>
#include <string>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <uuid/uuid.h>
#include "../proto/request.pb.h"

int connectToServer(const char* host, int port) {
    struct addrinfo hints{}, *res;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char portstr[6];
    snprintf(portstr, sizeof(portstr), "%d", port);

    if (getaddrinfo(host, portstr, &hints, &res)) {
        perror("getaddrinfo");
        return -1;
    }

    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
        perror("socket");
        freeaddrinfo(res);
        return -1;
    }

    if (connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        perror("connect");
        close(fd);
        freeaddrinfo(res);
        return -1;
    }

    freeaddrinfo(res);
    return fd;
}

bool writeNBytes(int fd, const void* buf, size_t n) {
    const char* p = static_cast<const char*>(buf);
    size_t left = n;
    while (left) {
        ssize_t w = ::send(fd, p, left, MSG_NOSIGNAL);
        if (w <= 0) return false;
        left -= w;
        p += w;
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

int main() {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    const char* host = "leo.sfc.keio.ac.jp";
    const int port = 7001;

    int fd = connectToServer(host, port);
    if (fd < 0) {
        std::cerr << "Failed to connect.\n";
        return 1;
    }

    // Construct write request
    request::Request req;
    req.set_recipient(request::Request::BATCHER);  // Adjust if needed
    req.set_client_id(getpid());

    auto* txn = req.add_transaction();
    txn->set_id(generateUUID());
    txn->set_client_id(getpid());

    auto* op = txn->add_operations();
    op->set_type(request::Operation::WRITE);
    op->set_key("1");
    op->set_value("example_value");

    // Serialize and send
    std::string data;
    req.SerializeToString(&data);
    uint32_t len = htonl(data.size());

    if (!writeNBytes(fd, &len, sizeof(len)) || !writeNBytes(fd, data.data(), data.size())) {
        std::cerr << "Failed to send request.\n";
        close(fd);
        return 1;
    }

    std::cout << "Write request sent successfully.\n";
    close(fd);
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
