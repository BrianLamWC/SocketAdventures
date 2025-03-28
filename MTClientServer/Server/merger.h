#ifndef MERGER_H
#define MERGER_H

#include <unordered_map>
#include <cstdint>

#include "transaction.h"
#include "../proto/request.pb.h"

class Merger
{
private:

    pthread_t merger_thread;
    std::unordered_map<int32_t, std::vector<Transaction>> partial_sequences;

public:
    Merger();
    void protoRequestToPartialSequence(const request::Request& req_proto);
    void temp();
};



#endif