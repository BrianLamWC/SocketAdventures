#include <unistd.h>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <stdlib.h>
#include <thread>
#include <arpa/inet.h>

#include "batcher.h"

namespace {
    // compile-time constant for a 5s window
    constexpr std::chrono::seconds ROUND_PERIOD{1};

    // initialized once at program startup
    const auto ROUND_EPOCH = std::chrono::system_clock::from_time_t(0);
}

void Batcher::batchRequests()
{
    using Clock = std::chrono::high_resolution_clock;
    static uint64_t total_txns = 0;
    static Clock::time_point start_all = Clock::now();

    while (true)
    {
        // align with the first boundary, can do this outside the loop once but repeating it might be safer?
        auto now = std::chrono::system_clock::now();
        auto since_0 = now - ROUND_EPOCH;
        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(since_0).count();

        // which window we’re currently in
        int64_t current_window = elapsed_seconds / ROUND_PERIOD.count();

        // the timestamp at which the next window begins
        auto next_timestamp = ROUND_EPOCH + std::chrono::seconds((current_window+1) * ROUND_PERIOD.count());

        // print current round
        //printf("BATCHER: in round %ld\n", current_window);

        auto t0_pop = Clock::now();
        batch_ = request_queue_.popAll();
        auto t1_pop = Clock::now();

        if (!batch_.empty()) {
            // 2) time processBatch_
            auto t0 = Clock::now();
            processBatch_();          // your existing code
            auto t1 = Clock::now();

            // 3) compute metrics
            auto pop_ms   = std::chrono::duration_cast<std::chrono::milliseconds>(t1_pop - t0_pop).count();
            auto proc_ms  = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
            auto N        = batch_.size();
            total_txns   += N;
            auto elapsed_all_s = std::chrono::duration_cast<std::chrono::seconds>(
                                Clock::now() - start_all
                                ).count();

            double batch_throughput = N / (proc_ms/1000.0);     // txn/sec for this batch
            double avg_throughput   = total_txns / double(elapsed_all_s>0?elapsed_all_s:1);

            // printf("BATCH popped %zu txns (in %lld ms), processed in %lld ms → %.0f tx/s, avg=%.0f tx/s\n",
            //     N, pop_ms, proc_ms, batch_throughput, avg_throughput);
        }

        batch_.clear();

        std::this_thread::sleep_until(next_timestamp);
    }
    
}

void Batcher::processBatch_()
{
    std::vector<request::Request> batch_for_partial_sequencer;

    for (request::Request& req_proto : batch_)
    {
        auto* txn = req_proto.mutable_transaction(0);
        txn->set_order(uuidv7());
        // int64_t stamp = lamport_clock.fetch_add(1) + 1;
        // txn->set_lamport_stamp(stamp);

        std::unordered_set<int32_t> target_peers;
        //printf("BATCHER: Transaction %s for client %d:\n", txn.id().c_str(), txn.client_id());

        bool validTransaction = true;
        for (const auto& op : txn->operations())
        {
            auto it = mockDB.find(op.key());
            if (it == mockDB.end()) {
                printf("  Key %s not found in the mock database\n", op.key().c_str());
                validTransaction = false;
                break;
            }
            
            const DataItem& data_item = it->second;
            // Add the primary copy ID to the set.
            target_peers.insert(data_item.primaryCopyID);
        }
        
        if (!validTransaction){
            printf("BATCHER: Transaction %s for client %d is invalid, skipping\n", 
                   txn->id().c_str(), txn->client_id());
            continue;
        }

        for (auto serverID : target_peers)
        {
            if (serverID == my_id)
            {
                batch_for_partial_sequencer.push_back(req_proto);
            }
            else
            {
                // Send to a remote server.
                sendTransaction_(*(txn), serverID);
            }
        }

    }

    if (!batch_for_partial_sequencer.empty()) {
        batcher_to_partial_sequencer_queue_.pushAll(batch_for_partial_sequencer);
    }
}

void Batcher::sendTransaction_(const request::Transaction& txn, const int32_t& id)
{
    std::string ip; 
    int port;

    // Find server details by id
    for (const auto& server : servers)
    {
        if (server.id == id)
        {
            ip = server.ip;
            port = server.port;
            break;
        }
    }
    
    if (ip.empty() || port == 0)
    {
        perror("Batcher::sendTransaction: ip empty or port = 0");
        return;
    }

    int connfd = setupConnection(ip, port);
    if (connfd < 0)
    {
        perror("Batcher::sendTransaction: connfd < 0");
        return;
    }
    
    // Create a Request message
    request::Request request;

    // Set the recipient, server_id, and optionally the client_id
    request.set_recipient(request::Request::PARTIAL);
    request.set_server_id(my_id);
    if (txn.has_client_id())
    {
        request.set_client_id(txn.client_id());
    }
    
    request.add_transaction()->CopyFrom(txn);
    
    // 1) serialize to string
    std::string serialized_request;
    if (!request.SerializeToString(&serialized_request)) {
        perror("SerializeToString failed");
        close(connfd);
        return;
    }

    // Print the request size
    size_t request_size = serialized_request.size();
    //printf("BATCHER: Serialized request size: %zu bytes\n", request_size);

    // 2) send 4-byte length prefix (network order)
    uint32_t len = htonl(static_cast<uint32_t>(request_size));
    if (!writeNBytes(connfd, &len, sizeof(len))) {
        perror("writeNBytes (length prefix) failed");
        close(connfd);
        return;
    }

    // Optionally, print that you’re now sending the framed message
    // printf("BATCHER: Sending %zu-byte request (plus 4-byte header) to %s:%d\n",
    //        request_size, ip.c_str(), port);

    if (!writeNBytes(connfd, serialized_request.data(), request_size)) {
        perror("writeNBytes (request) failed");
    }

    // Close the connection
    close(connfd);
}

std::string Batcher::uuidv7() {
    // random bytes
    std::random_device rd;
    std::array<uint8_t, 16> random_bytes;
    std::generate(random_bytes.begin(), random_bytes.end(), std::ref(rd));
    std::array<uint8_t, 16> value;
    std::copy(random_bytes.begin(), random_bytes.end(), value.begin());

    // current timestamp in ms
    auto now = std::chrono::system_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()
    ).count();

    // timestamp
    value[0] = (millis >> 40) & 0xFF;
    value[1] = (millis >> 32) & 0xFF;
    value[2] = (millis >> 24) & 0xFF;
    value[3] = (millis >> 16) & 0xFF;
    value[4] = (millis >> 8) & 0xFF;
    value[5] = millis & 0xFF;

    // version and variant
    value[6] = (value[6] & 0x0F) | 0x70;
    value[8] = (value[8] & 0x3F) | 0x80;

    char buf[37];
    std::snprintf(buf, sizeof(buf),
        "%02x%02x%02x%02x-"
        "%02x%02x-"
        "%02x%02x-"
        "%02x%02x-"
        "%02x%02x%02x%02x%02x%02x",
        value[0], value[1], value[2], value[3],
        value[4], value[5],
        value[6], value[7],
        value[8], value[9],
        value[10], value[11], value[12], value[13], value[14], value[15]
    );
    return std::string(buf);

}

// Constructor
Batcher::Batcher()
{

    if (pthread_create(&batcher_thread, NULL, [](void* arg) -> void* {
            static_cast<Batcher*>(arg)->Batcher::batchRequests();
            return nullptr;
        }, this) != 0) 
    {
        threadError("Error creating batcher thread");
    }
    
    pthread_detach(batcher_thread);

}
