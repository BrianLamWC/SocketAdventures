#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <iostream>

#define SERVER_ADDRESS "localhost"

void error(const char *msg)
{
    perror(msg);
    exit(0);
}

int main(int argc, char *argv[])
{

    if (argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << " <server port> <name>\n";
        return 1;
    }
    
    int server_port = std::stoi(argv[1]);
    char *name = argv[2];

    int sockfd;
    struct sockaddr_in serv_addr = {};
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0)
    {
        error("error opening socket");
    }
        
    server = gethostbyname(SERVER_ADDRESS);

    if (server == NULL)
    {
        error("error resolving server address");
    }

    serv_addr.sin_family = AF_INET;

    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);

    serv_addr.sin_port = htons(server_port);

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        error("error connecting");
    }

    char buffer[256];
    int received_bytes, sent_bytes;

    while (1)
    {

        memset(buffer, 0, sizeof(buffer));
        snprintf(buffer, sizeof(buffer) - 1, "Hello from %s!", name);

        sent_bytes = write(sockfd, buffer, strlen(buffer));

        if (sent_bytes < 0)
        {
            error("error writing to socket");
        }

        memset(buffer, 0, sizeof(buffer));

        // last byte will always be 0
        received_bytes = read(sockfd, buffer, 255);

        if (received_bytes < 0)
        {
            error("error reading from socket");
        }

        printf("Received: %s\n", buffer);

        sleep(3);
    }
    
    close(sockfd);

    return 0;
}