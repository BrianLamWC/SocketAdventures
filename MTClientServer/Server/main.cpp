#include <unistd.h>
#include <pthread.h>
#include <iostream>

#include "server.h"
#include "client.h"
#include "utils.h"
#include "batcher.h"
#include "partialSequencer.h"
#include "merger.h"
#include "insert.h"

int main(int argc, char *argv[])
{

    if (argc != 4)
    {
        std::cerr << "Usage: " << argv[0] << " <port for peers> <port for clients>\n";
        return 1;
    }

    peer_port = std::stoi(argv[1]);
    int client_port = std::stoi(argv[2]);
    my_id = std::stoi(argv[3]);
    
    // setup mockdb
    setupMockDB();

    // get list of servers
    getServers();
    int num_servers = servers.size();

    factory();
    insertAlgorithm();

    // run batcher
    Batcher batcher;

    // run partial sequencer
    PartialSequencer partial_sequencer;

    // run merger
    Merger merger;

    // set up listening sockets
    int peer_listenfd = setupListenfd(peer_port);
    int client_listenfd = setupListenfd(client_port);

    // listening for incoming connections
    listen(peer_listenfd, 5);
    listen(client_listenfd, 5);

    printf("Listening for peers on port %d\n", peer_port);
    printf("Listening for clients on port %d\n", client_port);

    // start listeners
    Listener peer_listener(connectionType::PEER, peer_listenfd, &partial_sequencer, &merger);
    Listener client_listener(connectionType::CLIENT, client_listenfd, &partial_sequencer, &merger);

    // arguments for pinger thread
    Pinger pinger(&servers, num_servers, peer_port);

    while (true) {
        pause(); // Blocks until a signal is received (e.g., SIGINT)
    }

    close(client_listenfd);
    close(peer_listenfd);

    return 0;
}