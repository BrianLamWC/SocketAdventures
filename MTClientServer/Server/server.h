#ifndef SERVER_H
#define SERVER_H

#include <string>
#include <vector>
#include <netinet/in.h>

#include "utils.h"
#include "partialSequencer.h"
#include "merger.h"

struct ServerArgs
{
    int connfd;
    struct sockaddr_in server_addr;
    PartialSequencer* partial_sequencer;
    Merger* merger;
};

void* handlePeer(void *server_args);
void* peerListener(void *args);

#endif // SERVER_H