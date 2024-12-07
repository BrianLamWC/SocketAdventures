#ifndef SERVER_H
#define SERVER_H

#include <string>
#include <vector>
#include <netinet/in.h>
#include "utils.h"

struct ServerArgs
{
    int connfd;
    struct sockaddr_in server_addr;
};

void* handlePeer(void *server_args);
void* peerListener(void *args);

#endif // SERVER_H