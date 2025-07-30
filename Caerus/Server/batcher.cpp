#include <unistd.h>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <stdlib.h>
#include <thread>
#include <arpa/inet.h>
#include <fstream>

#include "batcher.h"

namespace
{
    // compile-time constant for a 100ms window
    constexpr std::chrono::milliseconds ROUND_PERIOD{100};

    static thread_local std::mt19937_64 rng{std::random_device{}()};

    static thread_local std::uniform_int_distribution<int32_t> dist(
        std::numeric_limits<int32_t>::min(),
        std::numeric_limits<int32_t>::max());
}

void Batcher::batchRequests()
{
    // Wait until logical clock is ready
    while (!LOGICAL_EPOCH_READY.load())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    using Clock = std::chrono::high_resolution_clock;

    uint64_t total_txns = 0;
    std::chrono::nanoseconds::rep ns_total_stamp_time = 0;
    std::chrono::nanoseconds::rep ns_elapsed_time = 0;

    while (true)
    {
        auto now = std::chrono::steady_clock::now();
        auto since_0 = now - LOGICAL_EPOCH;
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(since_0).count();

        current_window = elapsed_ms / ROUND_PERIOD.count();

        auto next_timestamp = LOGICAL_EPOCH + std::chrono::milliseconds((current_window + 1) * ROUND_PERIOD.count());

        // Pull all transactions from the request queue
        batch = request_queue_.popAll();

        // // Log the received transactions
        // if (!batch.empty())
        // {
        //     std::ofstream log_file("batcher_received_log_" + std::to_string(my_id) + ".log", std::ios::app);
        //     if (log_file)
        //     {
        //         log_file << "Transactions Received by Batcher:\n";
        //         for (const auto &req : batch)
        //         {
        //             log_file << "  Transaction ID: " << req.transaction(0).id() << "\n";
        //             log_file << "  Client ID: " << req.transaction(0).client_id() << "\n";
        //             log_file << "  Operations:\n";
        //             for (const auto &op : req.transaction(0).operations())
        //             {
        //                 log_file << "    - Type: " << (op.type() == request::Operation::WRITE ? "WRITE" : "READ")
        //                          << ", Key: " << op.key();
        //                 if (op.type() == request::Operation::WRITE)
        //                 {
        //                     log_file << ", Value: " << op.value();
        //                 }
        //                 log_file << "\n";
        //             }
        //         }
        //         log_file << "----------------------------------------\n";
        //     }
        //     else
        //     {
        //         std::cerr << "Failed to open log file for batcher " << my_id << "\n";
        //     }
        // }

        if (!batch.empty())
        {
            auto t0 = Clock::now();
            processBatch(ns_total_stamp_time);
            auto t1 = Clock::now();
            ns_elapsed_time += std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

            total_txns += batch.size();
        }

        batch.clear();

        if (total_txns >= 1000000)
        {
            double avg_ns = double(ns_total_stamp_time) / double(total_txns);
            double throughput = double(total_txns) * 1e9 / double(ns_elapsed_time);
            double elapsed_ms = double(ns_elapsed_time) / 1e6;

            printf(
                "BATCHER: Stamped %llu txns in %.3f ms: avg stamp = %.1f ns/txn, throughput = %.0f tx/s\n",
                (unsigned long long)total_txns,
                elapsed_ms,
                avg_ns,
                throughput);

            total_txns = 0;
            ns_elapsed_time = 0;
            ns_total_stamp_time = 0;
        }

        std::this_thread::sleep_until(next_timestamp);
    }
}

void Batcher::processBatch(std::chrono::nanoseconds::rep &ns_total_stamp_time_)
{

    std::vector<request::Request> batch_for_partial_sequencer;
    batch_for_partial_sequencer.reserve(batch.size());

    using Clock = std::chrono::high_resolution_clock;

    for (request::Request &req_proto : batch)
    {
        auto t0s = Clock::now();

        auto *txn = req_proto.mutable_transaction(0);
        // stamp with random number, convert to string
        txn->set_order(std::to_string(dist(rng)));

        auto t1s = Clock::now();

        // accumulate the stamping time
        ns_total_stamp_time_ += std::chrono::duration_cast<std::chrono::nanoseconds>(t1s - t0s).count();

        // figure out which servers to send this transaction to
        std::unordered_set<int32_t> target_peers;
        bool validTransaction = true;
        for (const auto &op : txn->operations())
        {
            auto it = mockDB.find(op.key());
            if (it == mockDB.end())
            {
                printf("  Key %s not found in the mock database\n", op.key().c_str());
                validTransaction = false;
                break;
            }

            const DataItem &data_item = it->second;
            target_peers.insert(data_item.primaryCopyID);
        }

        if (!validTransaction)
        {
            printf("BATCHER: Transaction %s for client %d is invalid, skipping\n",
                   txn->id().c_str(), txn->client_id());
            continue;
        }

        for (auto target_id : target_peers)
        {
            // clone the original request
            request::Request req = req_proto;
            req.set_recipient(request::Request::PARTIAL);
            req.set_server_id(my_id);
            req.set_target_server_id(target_id);
            req.set_batcher_round(current_window);

            if (target_id == my_id)
            {
                batch_for_partial_sequencer.push_back(req);
            }
            else
            {
                outbound_queue.push(req);
            }
        }
    }

    batch_cv.notify_all();

    if (!batch_for_partial_sequencer.empty()) // has to have at least one transaction because transactions are always sent to nodes that have the primary copy of one of the keys in
    {
        // // Log the local pushes
        // std::ofstream log_file("batcher_local_push_log_" + std::to_string(my_id) + ".log", std::ios::app);
        // if (log_file)
        // {
        //     log_file << "Pushing local transactions to partial sequencer:\n";
        //     for (const auto &req : batch_for_partial_sequencer)
        //     {
        //         log_file << "  Transaction ID: " << req.transaction(0).id() << "\n";
        //         log_file << "  batcher_round: " << req.batcher_round() << "\n";
        //         log_file << "  Operations:\n";
        //         for (const auto &op : req.transaction(0).operations())
        //         {
        //             log_file << "    - Type: " << (op.type() == request::Operation::WRITE ? "WRITE" : "READ")
        //                      << ", Key: " << op.key();
        //             if (op.type() == request::Operation::WRITE)
        //             {
        //                 log_file << ", Value: " << op.value();
        //             }
        //             log_file << "\n";
        //         }
        //     }
        //     log_file << "----------------------------------------\n";
        // }
        // else
        // {
        //     std::cerr << "Failed to open log file for batcher " << my_id << "\n";
        // }

        batcher_to_partial_sequencer_queue_.pushAll(batch_for_partial_sequencer);
    }
}

void Batcher::sendTransaction(request::Request &req_proto)
{
    int target_id = req_proto.target_server_id();
    int &connfd = partial_sequencer_fds[target_id];

    // (re)connect on-demand if we lost it
    if (connfd < 0)
    {
        server target = target_peers[target_id];

        while ((connfd = setupConnection(target.ip, target.port)) < 0)
        {
            std::cerr << "Batcher " << my_id << ": reconnect to peer " << target_id << " failed, retrying in 1s…\n";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        partial_sequencer_fds[target_id] = connfd; // update the fd in the map
    }

    req_proto.set_recipient(request::Request::PARTIAL);
    req_proto.set_server_id(my_id);

    // // Log the transaction details
    // std::ofstream log_file("batcher_transaction_log_" + std::to_string(my_id) + ".log", std::ios::app);
    // if (log_file)
    // {
    //     log_file << "Sending transaction to node " << target_id << ":\n";
    //     log_file << "  Server ID: " << req_proto.server_id() << "\n";
    //     log_file << "  Target Server ID: " << req_proto.target_server_id() << "\n";
    //     log_file << "  Transactions:\n";

    //     for (const auto &txn : req_proto.transaction())
    //     {
    //         log_file << "    Transaction ID: " << txn.id() << "\n";
    //         log_file << "    Operations:\n";
    //         for (const auto &op : txn.operations())
    //         {
    //             log_file << "      - Type: " << (op.type() == request::Operation::WRITE ? "WRITE" : "READ")
    //                      << ", Key: " << op.key();
    //             if (op.type() == request::Operation::WRITE)
    //             {
    //                 log_file << ", Value: " << op.value();
    //             }
    //             log_file << "\n";
    //         }
    //     }
    //     log_file << "----------------------------------------\n";
    // }
    // else
    // {
    //     std::cerr << "Failed to open log file for batcher " << my_id << "\n";
    // }

    std::string serialized_request;
    if (!req_proto.SerializeToString(&serialized_request))
    {
        perror("SerializeToString failed");
        close(connfd);
        return;
    }

    uint32_t netlen = htonl(uint32_t(serialized_request.size()));
    if (!writeNBytes(connfd, &netlen, sizeof(netlen)) ||
        !writeNBytes(connfd, serialized_request.data(), serialized_request.size()))
    {
        perror("writeNBytes failed");
        // connection broken, force reconnect
        close(connfd);
        connfd = -1;
    }
}

std::string Batcher::uuidv7()
{
    static thread_local std::mt19937_64 rng(std::random_device{}());
    static thread_local std::uniform_int_distribution<uint64_t> dist;

    std::array<uint8_t, 16> value;

    // current timestamp in milliseconds
    auto now = std::chrono::system_clock::now();
    uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                          now.time_since_epoch())
                          .count();

    // UUIDv7 timestamp (first 48 bits)
    value[0] = (millis >> 40) & 0xFF;
    value[1] = (millis >> 32) & 0xFF;
    value[2] = (millis >> 24) & 0xFF;
    value[3] = (millis >> 16) & 0xFF;
    value[4] = (millis >> 8) & 0xFF;
    value[5] = millis & 0xFF;

    // Random bits
    uint64_t rand1 = dist(rng);
    uint64_t rand2 = dist(rng);

    // rand_a (12 bits) and version (4 bits)
    value[6] = 0x70 | ((rand1 >> 56) & 0x0F); // UUIDv7 version = 0111
    value[7] = (rand1 >> 48) & 0xFF;

    // Variant bits (2 bits = 10xxxxxx) + rand_b (62 bits)
    value[8] = 0x80 | ((rand1 >> 40) & 0x3F);
    value[9] = (rand1 >> 32) & 0xFF;
    value[10] = (rand1 >> 24) & 0xFF;
    value[11] = (rand1 >> 16) & 0xFF;
    value[12] = (rand1 >> 8) & 0xFF;
    value[13] = rand1 & 0xFF;
    value[14] = (rand2 >> 8) & 0xFF;
    value[15] = rand2 & 0xFF;

    // Format as string
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
                  value[10], value[11], value[12], value[13], value[14], value[15]);

    return std::string(buf);
}

// Constructor
Batcher::Batcher()
{

    std::ofstream init_local_log("batcher_local_push_log_" + std::to_string(my_id) + ".log", std::ios::out | std::ios::trunc);
    std::ofstream init_sent_log("batcher_transaction_log_" + std::to_string(my_id) + ".log", std::ios::out | std::ios::trunc);
    std::ofstream init_recv_log("batcher_received_log_" + std::to_string(my_id) + ".log", std::ios::out | std::ios::trunc);

    if (pthread_create(&sender_thread, NULL, [](void *arg) -> void *
                       {
            static_cast<Batcher*>(arg)->Batcher::sendTransactions();
            return nullptr; }, this) != 0)
    {
        threadError("Error creating sender thread");
    }

    pthread_detach(sender_thread);

    if (pthread_create(&batcher_thread, NULL, [](void *arg) -> void *
                       {
            static_cast<Batcher*>(arg)->Batcher::batchRequests();
            return nullptr; }, this) != 0)
    {
        threadError("Error creating batcher thread");
    }

    pthread_detach(batcher_thread);

    for (auto &server : servers)
    {
        if (server.id != my_id)
        {
            target_peers[server.id] = server;
            partial_sequencer_fds[server.id] = -1;
        }
    }

    printf("Batcher initialized with %zu target peers\n", target_peers.size());
}

void Batcher::sendTransactions()
{

    while (true)
    {
        std::unique_lock<std::mutex> lk(batch_mutex);
        batch_cv.wait(lk, [&]
                      { return !outbound_queue.empty(); });
        auto req = outbound_queue.pop(); // thread-safe pop
        lk.unlock();

        sendTransaction(req);
    }
}