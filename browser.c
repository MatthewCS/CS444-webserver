#include "net_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <arpa/inet.h>

#define COOKIE_PATH "./browser.cookie"

static bool browser_on = true;  // Determines if the browser is on/off.
static int server_socket_fd;    // The socket file descriptor of the server that is currently being connected.
static int session_id;          // The session ID of the session on the server that is currently being accessed.

// Reads the user input from stdin.
// If the input is "EXIT" or "exit",
// changes the browser switch to false.
void read_user_input(char message[]);

// Loads the cookie from the disk and gets the session ID
// if there exists one.
// Otherwise, assigns the session ID to be -1.
void load_cookie();

// Saves the session ID to the cookie on the disk.
void save_cookie();

// Interacts with the server to get or confirm
// the final session ID.
void register_server();

// Listens to the server.
// Keeps receiving and printing the messages from the server.
void server_listener();

// Starts the browser.
// Sets up the connection, start the listener thread,
// and keeps a loop to read in the user's input and send it out.
void start_browser(const char host_ip[], int port);

/**
 * Reads the user input from stdin. If the input is "EXIT" or "exit",
 * changes the browser switch to false.
 *
 * @param message an array to store the user input
 */
void read_user_input(char message[]) {
    fgets(message, BUFFER_LEN, stdin);

    if (message[strlen(message) - 1] == '\n') {
        message[strlen(message) - 1] = '\0';
    }

    if ((strcmp(message, "EXIT") == 0) || (strcmp(message, "exit") == 0)) {
        browser_on = false;
    }
}

/**
 * Loads the cookie from the disk and gets the session ID if there exists one.
 * Otherwise, assigns the session ID to be -1.
 */
void load_cookie() {
    // TODO: For Part 1.2, write your file operation code here.
    // Hint: The file path of the cookie is stored in COOKIE_PATH.
    session_id = -1; // You may move this line to anywhere inside this fucntion.
}

/**
 * Saves the session ID to the cookie on the disk.
 */
void save_cookie() {
    // TODO: For Part 1.2, write your file operation code here.
    // Hint: The file path of the cookie is stored in COOKIE_PATH.
}

/**
 * Interacts with the server to get or confirm the final session ID.
 */
void register_server() {
    char message[BUFFER_LEN];
    sprintf(message, "%d", session_id);
    send_message(server_socket_fd, message);

    receive_message(server_socket_fd, message);
    session_id = strtol(message, NULL, 10);
}

/**
 * Listens to the server; keeps receiving and printing the messages from the server.
 */
void server_listener() {
    // TODO: For Part 2.3, uncomment the loop code that was commented out
    //  when you are done with multithreading.

    // while (browser_on) {

    char message[BUFFER_LEN];
    receive_message(server_socket_fd, message);

    // TODO: For Part 3.1, add code here to print the error message.

    puts(message);

    //}
}

/**
 * Starts the browser. Sets up the connection, start the listener thread,
 * and keeps a loop to read in the user's input and send it out.
 * 
 * @param host_ip the host ip to connect
 * @param port the host port to connect
 */
void start_browser(const char host_ip[], int port) {
    // Loads the cookies if there exists one on the disk.
    load_cookie();

    // Creates the socket.
    server_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_fd < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Connects to the server via socket.
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(host_ip);
    server_addr.sin_port = htons(port);

    if (connect(server_socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) < 0) {
        perror("Socket connect failed");
        exit(EXIT_FAILURE);
    }
    printf("Connected to %s:%d.\n", host_ip, port);

    // Gets the final session ID.
    register_server();
    printf("Running Session #%d:\n", session_id);

    // Saves the session ID to the cookie on the disk.
    save_cookie();

    // Main loop to read in the user's input and send it out.
    while (browser_on) {
        char message[BUFFER_LEN];
        read_user_input(message);
        send_message(server_socket_fd, message);

        // Starts the listener thread.
        // TODO: For Part 2.3, move server_listener() out of the loop and
        //  creat a thread to run it.
        // Hint: Should we place server_listener() before or after the loop?
        server_listener();
    }

    // Closes the socket.
    close(server_socket_fd);
    printf("Closed the connection to %s:%d.\n", host_ip, port);
}

/**
 * The main function for the browser.
 *
 * @param argc the number of command-line arguments passed by the user
 * @param argv the array that contains all the arguments
 * @return exit code
 */
int main(int argc, char *argv[]) {
    char *host_ip = DEFAULT_HOST_IP;
    int port = DEFAULT_PORT;

    if (argc == 1) {
    } else if ((argc == 3)
               && ((strcmp(argv[1], "--host") == 0) || (strcmp(argv[1], "-h") == 0))) {
        host_ip = argv[2];

    } else if ((argc == 3)
               && ((strcmp(argv[1], "--port") == 0) || (strcmp(argv[1], "-p") == 0))) {
        port = strtol(argv[2], NULL, 10);

    } else if ((argc == 5)
               && ((strcmp(argv[1], "--host") == 0) || (strcmp(argv[1], "-h") == 0))
               && ((strcmp(argv[3], "--port") == 0) || (strcmp(argv[3], "-p") == 0))) {
        host_ip = argv[2];
        port = strtol(argv[4], NULL, 10);

    } else {
        puts("Invalid arguments.");
        exit(EXIT_FAILURE);
    }

    if (port < 1024) {
        puts("Invalid port.");
        exit(EXIT_FAILURE);
    }

    // Starts the browser using the given host IP and port
    start_browser(host_ip, port);

    exit(EXIT_SUCCESS);
}
