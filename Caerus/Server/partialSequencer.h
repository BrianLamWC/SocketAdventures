#ifndef PARTIALSEQUENCER_H
#define PARTIALSEQUENCER_H

#include <vector>   

#include "transaction.h"
#include "queueTS.h"
#include "../proto/request.pb.h"
#include "utils.h"

class PartialSequencer
{
private:

    int32_t next_round_{0};
    
    std::vector<Transaction> partial_sequence;
    std::vector<request::Request> transactions_received;
    request::Request partial_sequence_;
    pthread_t partial_sequencer_thread;
        
    std::unordered_map<int, server> target_peers;
    std::unordered_map<int,int> merger_fds; // id to fd mapping for merger

public:
    PartialSequencer();
    void processPartialSequence();
    void processPartialSequence2();
    void processPartialSequence3();
    void pushReceivedTransactionIntoPartialSequence(const request::Request& req_proto);
    void sendPartialSequence();
};

#endif