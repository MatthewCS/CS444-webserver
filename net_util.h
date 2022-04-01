//
// Created by xinchaosong on 3/26/22.
//

#ifndef PROJECT_NETWORK_H
#define PROJECT_NETWORK_H

#include <sys/socket.h>

#define DEFAULT_HOST_IP "127.0.0.1"
#define DEFAULT_PORT 7000
#define BUFFER_LEN 1024

// Sends the message through socket.
ssize_t send_message(int socket_fd, const char message[]);

// Receives the message through socket.
ssize_t receive_message(int socket_fd, char message[]);

#endif //PROJECT_NETWORK_H
