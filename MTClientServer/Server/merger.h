#ifndef MERGER_H
#define MERGER_H

#include <unordered_map>
#include "transaction.h"
#include "../proto/request.pb.h"

class Merger
{
private:

    pthread_t merger_thread;
    std::unordered_map<std::string, std::vector<Transaction>> partial_sequences;

public:
    Merger();
    void protoToPartialSequence(const request::Request& req_proto);
    void temp();
};



#endif