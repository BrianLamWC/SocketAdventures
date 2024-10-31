#include "server.h"
#include "client.h"
#include "utils.h"
#include <unistd.h>
#include <pthread.h>
#include <iostream>

int main(int argc, char *argv[])
{

    if (argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << " <port for servers> <port for clients>\n";
        return 1;
    }

    int server_port = std::stoi(argv[1]);
    int client_port = std::stoi(argv[2]);

    // get list of servers
    std::vector<server> servers = getServers();
    int num_servers = servers.size();

    // set up listening sockets
    int server_listenfd = setupListenfd(server_port);
    int client_listenfd = setupListenfd(client_port);

    // listening for incoming connections
    listen(server_listenfd, 5);
    listen(client_listenfd, 5);

    printf("Listening for servers on port %d\n", server_port);
    printf("Listening for clients on port %d\n", client_port);

    // arguments for listener threads
    ListenerThreadsArgs client_listener_args = {client_listenfd};
    ListenerThreadsArgs server_listener_args = {server_listenfd};

    // create listener threads
    pthread_t client_listener_thread, server_listener_thread;

    if (pthread_create(&client_listener_thread, NULL, clientListener, (void*)&client_listener_args) != 0)
    {
        error("error creating client listener thread");
    }

    if (pthread_create(&server_listener_thread, NULL, serverListener, (void*)&server_listener_args) != 0)
    {
        error("error creating server listener thread");
    }

    // arguments for pinger thread
    PingerThreadArgs pinger_args = { &servers, num_servers, server_port};

    // create a thread to handle pinging servers periodically
    pthread_t pinger_thread;
    if (pthread_create(&pinger_thread, NULL, pingServers, (void *)&pinger_args) != 0)
    {
        error("error creating thread for pinging servers");
    }

    // detach the pinger thread so that resources are automatically reclaimed
    pthread_detach(pinger_thread);

    pthread_join(client_listener_thread, NULL);
    pthread_join(server_listener_thread, NULL);

    close(client_listenfd);
    close(server_listenfd);

    return 0;
}