#ifndef SERVER_H
#define SERVER_H

#include <string>
#include <vector>
#include <netinet/in.h>

#include "utils.h"
#include "partialSequencer.h"
#include "merger.h"
#include "logger.h"

struct ServerArgs
{
    int connfd;
    struct sockaddr_in server_addr;
    PartialSequencer* partial_sequencer;
    Merger* merger;
    Logger* logger;

};

void* handlePeer(void *server_args);
void* peerListener(void *args);

struct PeerListenerThreadsArgs
{
    int listenfd;
    PartialSequencer* partial_sequencer;
    Merger* merger;
    Logger* logger;
};

class PeerListener
{
private:
    PeerListenerThreadsArgs args;
public:

    PeerListener(int listenfd, PartialSequencer* partial_sequencer, Merger* merger, Logger* logger);

};

#endif // SERVER_H