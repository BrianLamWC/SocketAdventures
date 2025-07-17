#include <unistd.h>
#include <pthread.h>
#include <iostream>

#include "server.h"
#include "client.h"
#include "utils.h"
#include "batcher.h"
#include "partialSequencer.h"
#include "merger.h"
#include "graph.h"
#include "logger.h"


int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        std::cerr << "Usage: " << argv[0] << " <id>" << std::endl;
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);

    // if id = 1 then peer_port = 8001, if id = 2 then peer_port = 8002, etc.
    // if id = 1 then client_port = 7001, if id = 2 then client_port = 7002, etc.
    // this is to avoid port conflicts
    peer_port = 8000 + my_id;
    int client_port = 7000 + my_id;

    my_id = std::stoi(argv[1]);
 
    peer_port = 8001;
    int client_port = 7001;

    // setup mockdb
    setupMockDB();

    // get list of servers
    getServers();
    int num_servers = servers.size();

    

    // run batcher
    Batcher batcher;

    // run partial sequencer
    PartialSequencer partial_sequencer;

    // run merger
    Merger merger;
    
    // run logger
    Logger logger;

    // set up listening sockets
    int peer_listenfd = setupListenfd(peer_port);
    int client_listenfd = setupListenfd(client_port);

    // listening for incoming connections
    listen(peer_listenfd, 5);
    listen(client_listenfd, 5);

    printf("Listening for peers on port %d\n", peer_port);
    printf("Listening for clients on port %d\n", client_port);

    // start listeners
    PeerListener peer_listener(peer_listenfd, &partial_sequencer, &merger);
    ClientListener client_listener(client_listenfd, &logger);

    Coordinator coordinator;

    // arguments for pinger thread
    //Pinger pinger(&servers, num_servers, peer_port);

    while (true) {
        pause();
    }


    close(peer_listenfd);
    close(client_listenfd);

    return 0;
}

