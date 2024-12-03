#ifndef UTILS_H
#define UTILS_H

#include <vector>
#include <string>
#include "server.h"

#define SERVERLIST "servers.json"
#define MOCKDB "data.json"

void error(const char *msg);
int setupListenfd(int my_port);
bool setNonBlocking(int listenfd);
void threadError(const char *msg);
int setupConnection(const std::string& ip, int port);
void setupMockDB();

void getServers(); 

#endif // UTILS_H
