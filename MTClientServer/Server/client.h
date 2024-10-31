#ifndef CLIENT_H
#define CLIENT_H

#include <netinet/in.h>

struct ClientArgs {
    int connfd;
    struct sockaddr_in client_addr;
};

// Function prototypes
void* clientListener(void *args);
void* handleClient(void *client_args);

#endif // CLIENT_H
