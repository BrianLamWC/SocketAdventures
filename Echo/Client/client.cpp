#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#define PORTNUM 7001
#define SERVER_ADDRESS "localhost"

void error(const char *msg)
{
    perror(msg);
    exit(0);
}

int main(int argc, char *argv[])
{
    int sockfd, n;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    char buffer[256];

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0)
    {
        error("ERROR opening socket");
    }
        
    server = gethostbyname(SERVER_ADDRESS);

    if (server == NULL)
    {
        fprintf(stderr, "ERROR, no such host\n");
        exit(0);
    }

    bzero((char *)&serv_addr, sizeof(serv_addr));

    serv_addr.sin_family = AF_INET;

    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);

    serv_addr.sin_port = htons(PORTNUM);

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        error("ERROR connecting");
    }

    while (1)
    {
        printf("Please enter the message: ");

        bzero(buffer, 256);

        fgets(buffer, 255, stdin);

        n = write(sockfd, buffer, strlen(buffer));

        if (n < 0)
        {
            error("ERROR writing to socket");
        }

        bzero(buffer, 256);

        n = read(sockfd, buffer, 255);

        if (n < 0)
        {
            error("ERROR reading from socket");
        }

        printf("Received: %s\n", buffer);

    }
    
    close(sockfd);

    return 0;
}