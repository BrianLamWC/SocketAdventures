#ifndef BATCHER_H
#define BATCHER_H

#include <vector>
#include "server.h"

class Batcher
{
private:

    std::vector<Request> batch;
    static const int BATCH_SIZE = 10;

public:

    void batchRequests();
    void processBatch(const std::vector<Request>& batch);

};

#endif