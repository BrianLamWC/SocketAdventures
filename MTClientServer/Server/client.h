#ifndef CLIENT_H
#define CLIENT_H

#include <netinet/in.h>
#include "utils.h"
#include "batcher.h"

struct ClientListenerThreadsArgs
{
    int listenfd;
};

struct ClientArgs {
    int connfd;
    struct sockaddr_in client_addr;
};

// Function prototypes
void* clientListener(void *args);
void* handleClient(void *client_args);

class ClientListener
{
private:
    ClientListenerThreadsArgs args;
public:

    ClientListener(int listenfd);

};

#endif // CLIENT_H
