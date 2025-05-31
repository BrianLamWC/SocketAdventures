#ifndef LOGGER_H
#define LOGGER_H

#include <pthread.h>
#include <mutex>
#include <condition_variable>
#include <fstream>
#include <filesystem>

#include "queueTS.h" 
#include "transaction.h"
#include "utils.h"

extern Queue_TS<Transaction> merged_order;
extern std::mutex logging_mutex;
extern std::condition_variable logging_cv;

class Logger {
private:

    // thread for logging merged orders
    pthread_t logging_thread;

    // thread for dumping the database state
    pthread_t db_dump_thread;

    // thread shutdown flag
    bool shutdown = false;

public:
    Logger();
    ~Logger();

    void logMergedOrders();

};


#endif // LOGGER_H