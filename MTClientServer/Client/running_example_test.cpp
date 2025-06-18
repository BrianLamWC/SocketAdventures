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
#include "../Server/json.hpp"
using json = nlohmann::json;

#define SERVER_ADDRESS "localhost"

std::unordered_map<int, int> key_to_primary;
static std::mt19937 rng{std::random_device{}()};
std::vector<int> all_keys;  // initialized once at startup

// Global atomic counter for transaction orders
std::atomic<int32_t> globalTransactionCounter{1};
std::atomic<int32_t> MRT_count{0};
std::atomic<int32_t> SRT_count{0};
static std::atomic<uint64_t> sent_count{0};
static std::atomic<bool> start_flag{false};

// vector to hold average throughput
std::vector<double> throughputs;
std::atomic<bool> stop_monitor{false};

// list of servers
std::vector<std::string> hostnames = {
    "leo.sfc.keio.ac.jp",
    "aries.sfc.keio.ac.jp",
    "133.27.19.50"
};

struct TxnSpec {
    std::string hostname;
    request::Operation::OperationType type;
    std::vector<int> keys;
};

void loadMockDB(const std::string& filename) {
    std::ifstream ifs(filename);
    if (!ifs) {
        std::cerr << "Could not open mock DB file.\n";
        exit(1);
    }

    json db;
    ifs >> db;

    for (const auto& item : db["data_items"]) {
        int key = std::stoi(item["key"].get<std::string>());
        int server_id = item["primary_server_id"].get<int>();
        key_to_primary[key] = server_id;
        all_keys.push_back(key);
    }

    std::cout << "Loaded " << key_to_primary.size() << " keys.\n";
}

std::vector<int> getRandomKeys(int num_keys = 3) {
    std::uniform_int_distribution<size_t> dist(0, all_keys.size() - 1);
    std::unordered_set<int> key_set;

    while (key_set.size() < std::min(static_cast<size_t>(num_keys), all_keys.size())) {
        key_set.insert(all_keys[dist(rng)]);
    }

    return std::vector<int>(key_set.begin(), key_set.end());
}

int chooseEligibleServer(const std::vector<int>& keys) {
    std::unordered_set<int> server_candidates;
    for (int key : keys) {
        auto it = key_to_primary.find(key);
        if (it != key_to_primary.end()) {
            server_candidates.insert(it->second);
        }
    }

    if (server_candidates.empty()) {
        std::cerr << "No eligible server found for keys.\n";
        return -1;
    }

    if (server_candidates.size() > 1) {
       SRT_count.fetch_add(1, std::memory_order_relaxed);
    }else {
       MRT_count.fetch_add(1, std::memory_order_relaxed);
    }

    std::vector<int> options(server_candidates.begin(), server_candidates.end());
    std::uniform_int_distribution<size_t> dist(0, options.size() - 1);
    return options[dist(rng)];
}

TxnSpec generateTxn() {
    std::vector<int> keys = getRandomKeys();
    int server_id = chooseEligibleServer(keys);

    std::unordered_map<int, std::string> server_to_host = {
        {1, "leo.sfc.keio.ac.jp"},
        {2, "aries.sfc.keio.ac.jp"},
        {3, "133.27.19.50"}
    };

    return {
        .hostname = server_to_host[server_id],
        .type = request::Operation::WRITE,
        .keys = keys
    };
}


void error(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

bool writeNBytes(int fd, const void *buf, size_t n) {
    const char *p = static_cast<const char*>(buf);
    size_t left = n;
    while (left) {
        ssize_t w = ::send(fd, p, left, MSG_NOSIGNAL);
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

int connectOne(const char* host, int port) {
    struct addrinfo hints{}, *res;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    char portstr[6];
    snprintf(portstr, sizeof(portstr), "%d", port);
    if (getaddrinfo(host, portstr, &hints, &res))
        return -1;
    int fd = -1;
    for (auto p=res; p; p=p->ai_next) {
        fd = socket(p->ai_family,p->ai_socktype,p->ai_protocol);
        if (fd<0) continue;
        if (connect(fd,p->ai_addr,p->ai_addrlen)==0) break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

void senderThread(int thread_id)
{
    thread_local std::unordered_map<std::string, int> my_conns;
    const int target_port = 7001;

    for (const std::string& host : hostnames) {
        int fd = connectOne(host.c_str(), target_port);
        if (fd < 0) {
            fprintf(stderr, "thread %d: can't connect to %s:%d\n", thread_id, host.c_str(), target_port);
            exit(1);
        }
        my_conns[host] = fd;
    }

    while (!start_flag.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    while (sent_count.load(std::memory_order_relaxed) < 3'500'000) {
        TxnSpec txn = generateTxn();
        int fd = my_conns[txn.hostname];

        request::Request req;
        req.set_recipient(request::Request::BATCHER);
        req.set_client_id(getpid());

        auto *t = req.add_transaction();
        t->set_id(std::to_string(globalTransactionCounter.fetch_add(1, std::memory_order_relaxed)));
        t->set_client_id(getpid());

        for (int k : txn.keys) {
            auto *op = t->add_operations();
            op->set_type(txn.type);
            op->set_key(std::to_string(k));
            if (txn.type == request::Operation::WRITE) {
                std::uniform_int_distribution<int> value_dist(1000, 9999);
                op->set_value(std::to_string(value_dist(rng)));
            }
        }

        std::string serialized;
        req.SerializeToString(&serialized);
        uint32_t netlen = htonl(serialized.size());

        writeNBytes(fd, &netlen, sizeof(netlen));
        writeNBytes(fd, serialized.data(), serialized.size());

        sent_count.fetch_add(1, std::memory_order_relaxed);
        usleep(100); // simulate some delay between sends
    }

}


// a simple monitor that prints every second
void throughput_monitor() {
    using Clock = std::chrono::steady_clock;
    auto last_t = Clock::now();
    uint64_t last_count = sent_count.load(std::memory_order_relaxed);

    while (!stop_monitor.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        auto now = Clock::now();
        uint64_t now_count = sent_count.load(std::memory_order_relaxed);

        auto delta = now_count - last_count;
        double secs = std::chrono::duration<double>(now - last_t).count();

        double throughput = delta / secs;
        throughputs.push_back(throughput);

        printf("Client throughput: %.0f tx/s\n", throughput);
        
        // print the total count
        printf("Total transactions sent: %llu\n", (unsigned long long)now_count);

        last_t = now;
        last_count = now_count;
    }
}

int main(int argc, char *argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <num_threads>\n";
        return 1;
    }
    int num_threads = std::stoi(argv[1]);

    loadMockDB("../Server/data.json");

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        // emplace forwards them to the constructor of the element type
        threads.emplace_back(senderThread, i);
    }

    std::thread monitor_thread(throughput_monitor);

    // give all threads a moment to spin up and wait on start_flag
    std::this_thread::sleep_for(std::chrono::seconds(1));

    std::cout << "All threads ready, starting senders now!\n";
    start_flag.store(true, std::memory_order_release);

    for (auto &t : threads) t.join();

    // Signal the monitor to stop
    stop_monitor.store(true, std::memory_order_release);
    monitor_thread.join();

    // calculate average throughput
    double total_throughput = 0.0;
    for (double t : throughputs) {
        total_throughput += t;
    }
    total_throughput /= throughputs.size(); 
    std::cout << "Average throughput across all threads: " << total_throughput << " tx/s\n";
    std::cout << "Total transactions sent: " << sent_count.load(std::memory_order_relaxed) << "\n";
    std::cout << "MRT count: " << MRT_count.load(std::memory_order_relaxed) << "\n";
    std::cout << "SRT count: " << SRT_count.load(std::memory_order_relaxed) << "\n";
    std::cout << "Percentage of MRT: "
              << (static_cast<double>(MRT_count.load(std::memory_order_relaxed)) / 
                  (MRT_count.load(std::memory_order_relaxed) + SRT_count.load(std::memory_order_relaxed))) * 100
              << "%\n";


    sleep(30);
    std::cout << "Sending DUMP requests to servers...\n";
    // send dump request to the servers
    for (const std::string& host : hostnames) {
        std::cout << "Connecting to " << host << ":7001\n";
        int fd = connectOne(host.c_str(), 7001);
        if (fd < 0) {
            fprintf(stderr, "Can't connect to %s:%d\n", host.c_str(), 7001);
            continue;
        }
        request::Request req;
        req.set_recipient(request::Request::DUMP);
        req.set_client_id(getpid());    
        std::string serialized;
        req.SerializeToString(&serialized);
        uint32_t netlen = htonl(serialized.size());
        writeNBytes(fd, &netlen, sizeof(netlen));
        writeNBytes(fd, serialized.data(), serialized.size());
        close(fd);
        std::cout << "Sent DUMP request to " << host << "\n";
    }


    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}

