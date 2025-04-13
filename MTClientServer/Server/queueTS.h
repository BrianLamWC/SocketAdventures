#ifndef QUEUE_TS_H
#define QUEUE_TS_H

#include <mutex>
#include <deque>
#include <condition_variable>

#include "transaction.h"
#include "../proto/request.pb.h"

// QUEUES
template<typename T>
class Queue_TS
{
private:

    std::deque<T> q;
    std::mutex mtx;

public:

    void push(const T& val);
    bool empty();
    std::vector<T> popAll(); // for batcher and partial sequencer to pop all txns
    T pop(); // for merger to pop a batch

};

extern Queue_TS<request::Request> request_queue_;
extern Queue_TS<request::Request> batcher_to_partial_sequencer_queue_;
extern Queue_TS<request::Request> partial_sequencer_to_merger_queue_;

extern std::mutex partial_sequencer_to_merger_queue_mtx;
extern std::condition_variable partial_sequencer_to_merger_queue_cv;


#endif // QUEUE_TS_H