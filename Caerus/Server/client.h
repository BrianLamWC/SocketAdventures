#ifndef CLIENT_H
#define CLIENT_H

#include <netinet/in.h>
#include "utils.h"
#include "batcher.h"
#include "logger.h"
#include "merger.h"

struct ClientListenerThreadsArgs
{
    int listenfd;
    Logger* logger;
    Merger* merger; 
};

struct ClientArgs {
    int connfd;
    struct sockaddr_in client_addr;
    Logger* logger; 
    Merger* merger; 
};

// Function prototypes
void* clientListener(void *args);
void* handleClient(void *client_args);

class ClientListener
{
private:
    ClientListenerThreadsArgs args;
public:

    ClientListener(int listenfd, Logger* logger, Merger* merger);

};

#endif // CLIENT_H
