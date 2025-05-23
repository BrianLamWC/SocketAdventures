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
    void pushAll(const std::vector<T>& items);
    bool empty();
    std::vector<T> popAll(); 
    T pop(); 
    size_t size() {
        std::lock_guard<std::mutex> lock(mtx);
        return q.size();
    }
};

// queue for client requests to batcher
extern Queue_TS<request::Request> request_queue_;

// queue for batcher to partial sequencer
extern Queue_TS<request::Request> batcher_to_partial_sequencer_queue_;

// queue for partial sequencer to merger
extern Queue_TS<request::Request> partial_sequencer_to_merger_queue_;
extern std::mutex partial_sequencer_to_merger_queue_mtx;
extern std::condition_variable partial_sequencer_to_merger_queue_cv;


#endif // QUEUE_TS_H