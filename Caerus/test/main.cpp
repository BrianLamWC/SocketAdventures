#include <iostream>
#include <vector>
#include <string>
#include "../proto/request.pb.h"

size_t estimateSize(const request::Request& req) {
    return req.ByteSizeLong();  // Wire size (compressed), good estimate
}

int main() {
    std::vector<request::Request> vec;

    size_t total_bytes = 0;
    size_t CHUNK_SIZE = 10000;
    size_t MAX_TOTAL = 100000000;

    try {
        while (total_bytes < 64L * 1024 * 1024 * 1024) { // e.g. stop at 64 GB
            for (size_t i = 0; i < CHUNK_SIZE; ++i) {
                request::Request req;
                req.set_client_id(1);
                req.set_server_id(2);
                req.set_recipient(request::Request::BATCHER);
                req.set_round(5);

                auto* txn = req.add_transaction();
                txn->set_id("txid");
                txn->set_order("uuid");
                txn->set_client_id(999);

                auto* op = txn->add_operations();
                op->set_type(request::Operation::WRITE);
                op->set_key("x");
                op->set_value("y");

                total_bytes += estimateSize(req);
                vec.push_back(std::move(req));
            }

            std::cout << "Stored: " << vec.size() << " requests, ~" << (total_bytes / (1024 * 1024)) << " MB\r" << std::flush;
        }
    } catch (const std::bad_alloc&) {
        std::cout << "\nOut of memory! Total stored: " << (total_bytes / (1024 * 1024)) << " MB\n";
    }

    return 0;
}
