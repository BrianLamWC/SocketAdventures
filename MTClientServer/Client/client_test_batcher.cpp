#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <iostream>
#include <pthread.h>

#define SERVER_ADDRESS "localhost"

void error(const char *msg)
{
    perror(msg);
    pthread_exit(NULL);
}

void *clientThread(void *args)
{
    int server_port = *((int *)args);
    char name[50];
    snprintf(name, sizeof(name), "Client-%ld", pthread_self());

    while (true)
    {
        int sockfd;
        struct sockaddr_in serv_addr = {};
        struct hostent *server;

        // Create socket
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0)
        {
            error("error opening socket");
        }

        // Resolve server address
        server = gethostbyname(SERVER_ADDRESS);
        if (server == NULL)
        {
            error("error resolving server address");
        }

        serv_addr.sin_family = AF_INET;
        bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
        serv_addr.sin_port = htons(server_port);

        // Connect to server
        if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        {
            error("error connecting");
        }

        // Send and receive data
        char buffer[256];
        int received_bytes, sent_bytes;

        memset(buffer, 0, sizeof(buffer));
        snprintf(buffer, sizeof(buffer) - 1, "Hello from %s!", name);

        sent_bytes = write(sockfd, buffer, strlen(buffer));
        if (sent_bytes < 0)
        {
            error("error writing to socket");
        }

        memset(buffer, 0, sizeof(buffer));
        received_bytes = read(sockfd, buffer, 255);
        if (received_bytes < 0)
        {
            error("error reading from socket");
        }

        printf("%s received: %s\n", name, buffer);

        // Close the connection
        close(sockfd);

        // Sleep for 10 seconds before reconnecting
        sleep(10);
    }

    pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        std::cerr << "Usage: " << argv[0] << " <server port>\n";
        return 1;
    }

    int server_port = std::stoi(argv[1]);
    pthread_t threads[3];

    // Create 3 client threads
    for (int i = 0; i < 3; ++i)
    {
        if (pthread_create(&threads[i], NULL, clientThread, (void *)&server_port) != 0)
        {
            perror("error creating thread");
            exit(1);
        }
    }

    // Wait for all threads to finish (they won't, as they run indefinitely)
    for (int i = 0; i < 3; ++i)
    {
        pthread_join(threads[i], NULL);
    }

    return 0;
}
