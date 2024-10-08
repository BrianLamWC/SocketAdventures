/* A simple server in the internet domain using TCP
   The port number is passed as an argument */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define PORTNUM 3333

// Function to handle error messages and exit the program
void error(const char *msg)
{
    perror(msg); // Prints the error message to stderr
    exit(1);     // Exits the program with status 1 (error)
}

int main(int argc, char *argv[])
{
    int sockfd, newsockfd, portno;          // File descriptors for sockets and port number
    socklen_t clilen;                       // Size of the client address structure
    char buffer[256];                       // Buffer to store messages
    struct sockaddr_in serv_addr, cli_addr; // Structures for server and client address
    int n;                                  // Variable to store the number of bytes read/written

    // Create a socket: AF_INET for IPv4, SOCK_STREAM for TCP
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        error("ERROR opening socket"); // Handle socket creation error

    // Initialize the server address structure with zeros
    bzero((char *)&serv_addr, sizeof(serv_addr));

    // Convert the port number argument from string to integer
    portno = PORTNUM;

    // Set up the server address structure
    serv_addr.sin_family = AF_INET;         // Use IPv4
    serv_addr.sin_addr.s_addr = INADDR_ANY; // Accept connections from any IP
    serv_addr.sin_port = htons(portno);     // Convert port number to network byte order

    // Bind the socket to the server address and port
    if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        error("ERROR on binding"); // Handle binding error
    }
        
    // Listen for incoming connections, allowing up to 5 pending connections
    listen(sockfd, 5);

    printf("Listening on port %d\n", ntohs(serv_addr.sin_port));

    // Get the size of the client address structure
    clilen = sizeof(cli_addr);

    // Accept a connection from a client
    newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);
    if (newsockfd < 0)
    {
        error("ERROR on accept"); // Handle error during connection acceptance
    }

    while (1)
    {
        // Clear the buffer for receiving the message
        bzero(buffer, 256);

        // Read the message from the client
        n = read(newsockfd, buffer, 255);
        if (n < 0)
        {
            error("ERROR reading from socket"); // Handle reading error
        }

        if (n == 0)
        {
            printf("Client %d closed connection\n", newsockfd);
            break;
        }
        

        // Print the message received from the client
        printf("Client %d: %s\n", newsockfd,buffer);

        // Send a response to the client
        n = write(newsockfd, buffer, strlen(buffer));
        if (n < 0)
        {
            error("ERROR writing to socket"); // Handle writing error
        }
    }
    
    // Close the connection with the client
    close(newsockfd);

    // Close the server socket
    close(sockfd);

    return 0; // Exit the program successfully
}