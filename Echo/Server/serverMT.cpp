#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORTNUM 3333

void error(const char *msg)
{
    perror(msg);
    exit(1);
}

void handleClient(int newsockfd, struct sockaddr_in client_addr)
{
    char buffer[256];
    int buffer_size = sizeof(buffer);
    int received_bytes, sent_bytes;

    // Get client's IP address and port
    char *client_ip = inet_ntoa(client_addr.sin_addr);
    int client_port = ntohs(client_addr.sin_port);

    printf("Client connected: %s:%d\n", client_ip, client_port);

    while (1)
    {
        // clear buffer 
        bzero(buffer, buffer_size);

        // read the message from the client
        received_bytes = read(newsockfd, buffer, 255); // leave one byte for null terminator

        if (received_bytes < 0)
        {
            error("error reading from socket"); 
        }

        if (received_bytes == 0)
        {
            printf("Client %s:%d closed their connection\n", client_ip, client_port);
            break;
        }

        // print received message
        printf("Client %s:%d: %s\n", client_ip, client_port, buffer);

        // send the received message back to the client (echo)
        sent_bytes = write(newsockfd, buffer, strlen(buffer));
        if (sent_bytes < 0)
        {
            error("error writing to socket");
        }
    }

    // Close the client socket after the communication ends
    close(newsockfd);
}

int main()
{
    int sockfd, newsockfd;
    socklen_t client_addrlen;
    struct sockaddr_in server_addr, client_addr;

    client_addrlen = sizeof(client_addr);

    // create socket: AF_INET for IPv4 and SOCK_STREAM for TCP
    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0)
    {
        error("error creating socket");
    }

    // initialize the server address structure with zeros
    bzero((char *)&server_addr, sizeof(server_addr));

    // configure server address structure
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORTNUM);

    // bind socket to server address and port
    if (bind(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        error("ERROR on binding");
    }

    // Listen for incoming connections, allowing up to 5 pending connections
    listen(sockfd, 5);

    printf("Listening on port %d\n", ntohs(server_addr.sin_port));

    while (1)
    {
        // accept a client
        newsockfd = accept(sockfd, (struct sockaddr *)&client_addr, &client_addrlen);
        if (newsockfd < 0)
        {
            error("error accepting a connection");
        }

        // fork a new process to handle the client
        pid_t pid = fork();

        if (pid < 0)
        {
            error("error forking");
        }
        else if (pid == 0)
        {
            // child process handles the client
            close(sockfd); // listening socket not needed anymore
            handleClient(newsockfd, client_addr);
            exit(0); // exit child process when done
        }
        else
        {
            // parent process continues to accept new clients
            close(newsockfd); // connection socket not needed anymore in parent process
        }
    }

    // Close the server socket in parent process
    close(sockfd);

    return 0; // Exit the program successfully
}
