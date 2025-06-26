#ifndef CLIENT_H
#define CLIENT_H

#include <netinet/in.h>
#include "utils.h"
#include "batcher.h"
#include "logger.h"

struct ClientListenerThreadsArgs
{
    int listenfd;
    Logger* logger;
};

struct ClientArgs {
    int connfd;
    struct sockaddr_in client_addr;
    Logger* logger;  // Pointer to logger for DUMP requests
};

// Function prototypes
void* clientListener(void *args);
void* handleClient(void *client_args);

class ClientListener
{
private:
    ClientListenerThreadsArgs args;
public:

    ClientListener(int listenfd, Logger* logger);

};

#endif // CLIENT_H
