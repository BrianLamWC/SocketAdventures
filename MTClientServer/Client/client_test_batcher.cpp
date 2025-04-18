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
#include <string>
#include <atomic>
#include <uuid/uuid.h>   // Include libuuid for generating UUIDs
#include "../proto/request.pb.h"

#define SERVER_ADDRESS "localhost"

// Global atomic counter for transaction orders
std::atomic<int32_t> globalTransactionCounter{0};

void error(const char *msg)
{
    perror(msg);
    pthread_exit(NULL);
}

// Function to generate a UUID as a string
std::string generateUUID() {
    uuid_t uuid;
    char uuid_str[37];  // 36 characters plus null terminator
    uuid_generate(uuid);
    uuid_unparse(uuid, uuid_str);
    return std::string(uuid_str);
}

void *clientThread(void *args)
{
    int server_port = *((int *)args);
    int client_id = static_cast<int>(pthread_self()); // Convert pthread_self() to an integer

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

        // Create a Request message
        request::Request request;

        // Set the recipient and client_id
        request.set_recipient(request::Request::BATCHER);
        request.set_client_id(client_id);

        // Create the Transaction and add Operations
        request::Transaction *transaction = request.add_transaction();

        // Generate a unique, ordered transaction order
        int32_t order = globalTransactionCounter.fetch_add(1);
        transaction->set_order(order);

        // Generate a UUID for the transaction id as a string.
        // (Assumes your proto Transaction message has a required string id field.)
        std::string uuid = generateUUID();
        transaction->set_id(uuid);
        transaction->set_client_id(client_id);

        // Add write operation W(1, 2)
        request::Operation *op1 = transaction->add_operations();
        op1->set_type(request::Operation::WRITE);
        op1->set_key("1");
        op1->set_value("2");

        // Add write operation W(2, 3)
        request::Operation *op2 = transaction->add_operations();
        op2->set_type(request::Operation::WRITE);
        op2->set_key("2");
        op2->set_value("3");

        // Add write operation W(3, 4)
        request::Operation *op3 = transaction->add_operations();
        op3->set_type(request::Operation::WRITE);
        op3->set_key("3");
        op3->set_value("4");

        // Serialize the Request message
        std::string serialized_request;
        if (!request.SerializeToString(&serialized_request))
        {
            error("error serializing request");
        }

        // Send serialized request
        int sent_bytes = write(sockfd, serialized_request.c_str(), serialized_request.size());
        if (sent_bytes < 0)
        {
            error("error writing to socket");
        }

        // Print client info and the generated UUID
        printf("%d sent a request with %d bytes. Transaction UUID: %s\n",
               client_id, sent_bytes, uuid.c_str());

        // Close the connection
        close(sockfd);

        // Sleep for 10 seconds before reconnecting
        sleep(1);
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

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    int server_port = std::stoi(argv[1]);
    pthread_t threads[1];

    // Create 3 client threads
    for (int i = 0; i < 1; ++i)
    {
        if (pthread_create(&threads[i], NULL, clientThread, (void *)&server_port) != 0)
        {
            perror("error creating thread");
            exit(1);
        }
    }

    // Wait for all threads to finish (they won't, as they run indefinitely)
    for (int i = 0; i < 1; ++i)
    {
        pthread_join(threads[i], NULL);
    }

    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}
