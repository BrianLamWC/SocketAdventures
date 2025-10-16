#include <vector>
#include "queueTS.h"

// Global instantiations:
Queue_TS<request::Request> request_queue_;

Queue_TS<request::Request> batcher_to_partial_sequencer_queue_;

Queue_TS<request::Request> partial_sequencer_to_merger_queue_;
std::mutex partial_sequencer_to_merger_queue_mtx;
std::condition_variable partial_sequencer_to_merger_queue_cv;

template<typename T>
void Queue_TS<T>::push(const T& val) {
    std::lock_guard<std::mutex> lock(mtx);
    q.push_back(val);  // use push_back for deque
}

template<typename T>
void Queue_TS<T>::pushAll(const std::vector<T>& items) {
    std::lock_guard<std::mutex> lock(mtx);
    for (const auto &v : items) {
      q.push_back(v);
    }
}

template <typename T>
bool Queue_TS<T>::empty() {
    std::lock_guard<std::mutex> lock(mtx);
    return q.empty();
}

template<typename T>
std::vector<T> Queue_TS<T>::popAll() {
    std::lock_guard<std::mutex> lock(mtx);
    std::vector<T> items;
    while (!q.empty()) {
        items.push_back(q.front());
        q.pop_front();  // use pop_front for deque
    }
    return items;
}

template <typename T>
T Queue_TS<T>::pop() {
    std::lock_guard<std::mutex> lock(mtx);
    if (q.empty()) {
        throw std::runtime_error("pop() called on empty queue");
    }
    T item = std::move(q.front());
    q.pop_front();  // use pop_front for deque
    return item;
}


template <typename T>
std::vector<T> Queue_TS<T>::snapshot() const {
    std::lock_guard<std::mutex> lk(mtx);            // your queue's mutex
    return std::vector<T>(q.begin(), q.end()); // your container
}

// Explicit template instantiations:
template class Queue_TS<request::Transaction>;
template class Queue_TS<Transaction>;
template class Queue_TS<std::vector<Transaction>>;

template class Queue_TS<std::vector<request::Request>>;
template class Queue_TS<request::Request>;
