#ifndef PARTIALSEQUENCER_H
#define PARTIALSEQUENCER_H

#include <vector>

#include "transaction.h"
#include "../proto/request.pb.h"


class PartialSequencer
{
private:
    
    std::vector<Transaction> partial_sequence;
    pthread_t partial_sequencer_thread;

public:
    PartialSequencer();
    void processPartialSequence();
    void pushReceivedTransactionIntoPartialSequence(const request::Request& req_proto);
    void sendPartialSequence(const std::string& ip, const int& port);
};


#endif