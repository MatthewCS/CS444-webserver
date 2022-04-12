#include "net_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <ctype.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>

#include <stdio.h>  // part 1
#include <fcntl.h>  // part 1
#include <regex.h>  // part 3.1
#include "inc/uthash.h" // part 3.2

#define NUM_VARIABLES 26
#define NUM_SESSIONS 128
#define NUM_BROWSER 128
#define DATA_DIR "./sessions"
#define SESSION_PATH_LEN 128

typedef struct browser_struct {
    bool in_use;
    int socket_fd;
    int session_id;
} browser_t;

typedef struct session_struct {
    bool in_use;
    bool variables[NUM_VARIABLES];
    double values[NUM_VARIABLES];
    // part 3.2
    UT_hash_handle hh;
} session_t;

// part 3.2
typedef struct session_ht_struct {
    int id;
    session_t* session;
    UT_hash_handle hh;
} session_ht_t;

static browser_t browser_list[NUM_BROWSER];                             // Stores the information of all browsers.
// TODO: For Part 3.2, convert the session_list to a simple hashmap/dictionary.
// static session_t session_list[NUM_SESSIONS];                            // Stores the information of all sessions.
static struct session_ht_struct *session_ht = NULL;
static pthread_mutex_t browser_list_mutex = PTHREAD_MUTEX_INITIALIZER;  // A mutex lock for the browser list.
static pthread_mutex_t session_list_mutex = PTHREAD_MUTEX_INITIALIZER;  // A mutex lock for the session list.

// Returns the string format of the given session.
// There will be always 9 digits in the output string.
void session_to_str(int session_id, char result[]);

// Determines if the given string represents a number.
bool is_str_numeric(const char str[]);

// Process the given message and update the given session if it is valid.
bool process_message(int session_id, const char message[]);

// Broadcasts the given message to all browsers with the same session ID.
void broadcast(int session_id, const char message[]);

// Gets the path for the given session.
void get_session_file_path(int session_id, char path[]);

// Loads every session from the disk one by one if it exists.
void load_all_sessions();

// Saves the given sessions to the disk.
void save_session(int session_id);

// Assigns a browser ID to the new browser.
// Determines the correct session ID for the new browser
// through the interaction with it.
int register_browser(int browser_socket_fd);

// Handles the given browser by listening to it,
// processing the message received,
// broadcasting the update to all browsers with the same session ID,
// and backing up the session on the disk.
void browser_handler(int browser_socket_fd);

// Starts the server.
// Sets up the connection,
// keeps accepting new browsers,
// and creates handlers for them.
void start_server(int port);

/**
 * Returns the string format of the given session.
 * There will be always 9 digits in the output string.
 *
 * @param session_id the session ID
 * @param result an array to store the string format of the given session;
 *               any data already in the array will be erased
 */
void session_to_str(int session_id, char result[]) {
    memset(result, 0, BUFFER_LEN);
    // session_t session = session_list[session_id];
    session_t* session;
    session_ht_t* session_ht_entry;
    HASH_FIND_INT(session_ht, &session_id, session_ht_entry);
    session = session_ht_entry->session;

    for (int i = 0; i < NUM_VARIABLES; ++i) {
        if (session->variables[i]) {
            char line[32];

            if (session->values[i] < 1000) {
                sprintf(line, "%c = %.6f\n", 'a' + i, session->values[i]);
            } else {
                sprintf(line, "%c = %.8e\n", 'a' + i, session->values[i]);
            }

            strcat(result, line);
        }
    }
}

/**
 * Determines if the given string represents a number.
 *
 * @param str the string to determine if it represents a number
 * @return a boolean that determines if the given string represents a number
 */
bool is_str_numeric(const char str[]) {
    if (str == NULL) {
        return false;
    }

    if (!(isdigit(str[0]) || (str[0] == '-') || (str[0] == '.'))) {
        return false;
    }

    int i = 1;
    while (str[i] != '\0') {
        if (!(isdigit(str[i]) || str[i] == '.')) {
            return false;
        }
        i++;
    }

    return true;
}

/**
 * Process the given message and update the given session if it is valid.
 *
 * @param session_id the session ID
 * @param message the message to be processed
 * @return a boolean that determines if the given message is valid
 */
bool process_message(int session_id, const char message[]) {
    char *token;
    int result_idx;
    double first_value;
    char symbol;
    double second_value;

    // TODO: For Part 3.1, write code to determine if the input is invalid and return false if it is.
    // Hint: Also need to check if the given variable does exist (i.e., it has been assigned with some value)
    // for the first variable and the second variable, respectively.
    regex_t regex;
    int re_return_val;
    char* re_pattern = " *[a-z] *= *[a-z0-9] *[+-/*] *[a-z0-9] *";

    re_return_val = regcomp(&regex, re_pattern, 0);
    re_return_val = regexec(&regex, message, 0, NULL, 0);

    // Was the regex pattern found or not?
    if(re_return_val != 0) {
        // not found
        printf("Input \"%s\" not matched!\n", message);
        return false;
    }

    // Part 3.2
    session_t* session;
    session_ht_t* session_ht_entry;
    HASH_FIND_INT(session_ht, &session_id, session_ht_entry);
    session = malloc(sizeof session);
    session = session_ht_entry->session;

    // Makes a copy of the string since strtok() will modify the string that it is processing.
    char data[BUFFER_LEN];
    strcpy(data, message);

    // Processes the result variable.
    token = strtok(data, " ");
    result_idx = token[0] - 'a';

    // Processes "=".
    token = strtok(NULL, " ");

    // Processes the first variable/value.
    token = strtok(NULL, " ");
    if (is_str_numeric(token)) {
        first_value = strtod(token, NULL);
    } else {
        int first_idx = token[0] - 'a';
        first_value = session->values[first_idx];
    }

    // Processes the operation symbol.
    token = strtok(NULL, " ");
    if (token == NULL) {
        session->variables[result_idx] = true;
        session->values[result_idx] = first_value;
        return true;
    }
    symbol = token[0];

    // Processes the second variable/value.
    token = strtok(NULL, " ");
    if (is_str_numeric(token)) {
        second_value = strtod(token, NULL);
    } else {
        int second_idx = token[0] - 'a';
        second_value = session->values[second_idx];
    }

    // No data should be left over thereafter.
    token = strtok(NULL, " ");

    session->variables[result_idx] = true;

    if (symbol == '+') {
        session->values[result_idx] = first_value + second_value;
    } else if (symbol == '-') {
        session->values[result_idx] = first_value - second_value;
    } else if (symbol == '*') {
        session->values[result_idx] = first_value * second_value;
    } else if (symbol == '/') {
        session->values[result_idx] = first_value / second_value;
    }

    return true;
}

/**
 * Broadcasts the given message to all browsers with the same session ID.
 *
 * @param session_id the session ID
 * @param message the message to be broadcasted
 */
void broadcast(int session_id, const char message[]) {
    for (int i = 0; i < NUM_BROWSER; ++i) {
        if (browser_list[i].in_use && browser_list[i].session_id == session_id) {
            send_message(browser_list[i].socket_fd, message);
        }
    }
}

/**
 * Gets the path for the given session.
 *
 * @param session_id the session ID
 * @param path the path to the session file associated with the given session ID
 */
void get_session_file_path(int session_id, char path[]) {
    sprintf(path, "%s/session%d.dat", DATA_DIR, session_id);
}

/**
 * Loads every session from the disk one by one if it exists.
 */
void load_all_sessions() {
    // TODO: For Part 1.1, write your file operation code here.
    // Hint: Use get_session_file_path() to get the file path for each session.
    //       Don't forget to load all of sessions on the disk.
    char session_path[SESSION_PATH_LEN];

    for(int id = 0; id < NUM_SESSIONS; ++id)
    {
        get_session_file_path(id, session_path);
        int fd = open(
            session_path,
            O_RDONLY,
            S_IRUSR
        );

        // Part 3.2
        session_t* session;
        session_ht_t* session_ht_entry;
        session_ht_entry = malloc(sizeof *session_ht_entry);
        session_ht_entry->id = id;
        HASH_ADD_INT(session_ht, id, session_ht_entry);
        session_ht_entry->session = malloc(sizeof *session);
        session = session_ht_entry->session;

        // did we fail to open the file?
        if(fd < 0)
        {
            continue;
        }

        char contents[512 * 3];
        int rc = read(fd, contents, sizeof(contents));

        // did we fail to read the file?
        if(rc < 0)
        {
            continue;
        }

        // printf(contents);
        char* line = contents;
        int line_num = 1;
        while(line)
        {
            char* next_line = strchr(line, '\n');
            // terminate if need be
            if(next_line)
                *next_line = '\0';

            // line #1
            if(line_num == 1)
            {
                session->in_use = atoi(line);
                ++line_num;

                // printf("%d\n", session_list[id].in_use);
            }
            // line #2
            else if(line_num == 2)
            {
                char val[1];
                char* ptr = strtok(line, " ");
                for(int i = 0; i < NUM_VARIABLES; ++i)
                {
                    sprintf(val, "%d", *ptr);
                    session->variables[i] = atoi(val - '0');
                    ptr = strtok(NULL, " ");

                    // printf("%d ", session_list[id].variables[i]);
                }
                // printf("\n");

                ++line_num;
            }
            // line #3
            else if(line_num == 3)
            {
                // char val[20];
                // char* ptr = strtok(line, " ");
                char* ptr;
                double val;
                for(int i = 0; i < NUM_VARIABLES; ++i)
                {
                    // not working
                    val = strtod(line, &ptr);
                    session->values[i] = val;
                    // printf("%lf ", session_list[id].values[i]);
                    line = ptr;
                }
                // printf("\n");

                ++line_num;
            }

            // restore newline
            if(next_line)
                *next_line = '\n';

            line = next_line ? (next_line + 1) : NULL;
        }
    }
}

/**
 * Saves the given sessions to the disk.
 *
 * @param session_id the session ID
 */
void save_session(int session_id) {
    // TODO: For Part 1.1, write your file operation code here.
    // Hint: Use get_session_file_path() to get the file path for each session.

    char session_path[SESSION_PATH_LEN];
    get_session_file_path(session_id, session_path);
    int fd = open(
        session_path,
        O_WRONLY | O_CREAT | O_TRUNC,
        S_IRUSR | S_IWUSR
    );

    // Part 3.2
    session_t* session;
    session_ht_t* session_ht_entry;
    HASH_FIND_INT(session_ht, &session_id, session_ht_entry);
    session = malloc(sizeof session);
    session = session_ht_entry->session;

    // did we open the file?
    if(fd >= 0)
    {
        char line1[512];
        char line2[512], *pos2 = line2;
        char line3[512], *pos3 = line3;
        char all_lines[512*3], *pos_all = all_lines;

        // format each line
        sprintf(line1, "%d\n", session->in_use);
        for(int i = 0; i < NUM_VARIABLES; ++i)
        {
            if(i)
            {
                pos2 += sprintf(pos2, " ");
                pos3 += sprintf(pos3, " ");
            }
            pos2 += sprintf(pos2, "%d", session->variables[i]);
            pos3 += sprintf(pos3, "%lf", session->values[i]);
        }

        pos2 += sprintf(pos2, "\n");
        pos3 += sprintf(pos3, "\n\0");

        pos_all += sprintf(all_lines, "%s%s%s", line1, line2, line3);

        // write our output
        int writerc = write(fd, all_lines, strlen(all_lines));
        // write() failed
        if(writerc != strlen(all_lines))
        {
            printf(
                "ERROR: write failed (session_id = %d, writerc = %d.)\n",
                session_id, writerc
            );
        }

        // close the session file
        fsync(fd);
        close(fd);
    }
    // failed to open the file
    else
    {
        printf(
            "ERROR: open failed (session_id = %d, fd = %d)\n",
            session_id,
            fd
        );
    }
}

/**
 * Assigns a browser ID to the new browser.
 * Determines the correct session ID for the new browser through the interaction with it.
 *
 * @param browser_socket_fd the socket file descriptor of the browser connected
 * @return the ID for the browser
 */
int register_browser(int browser_socket_fd) {
    int browser_id;

    // TODO: For Part 2.2, identify the critical sections where different threads may read from/write to
    //  the same shared static array browser_list and session_list. Place the lock and unlock
    //  code around the critical sections identified.

    for (int i = 0; i < NUM_BROWSER; ++i) {
        if (!browser_list[i].in_use) {
            browser_id = i;
            browser_list[browser_id].in_use = true;
            browser_list[browser_id].socket_fd = browser_socket_fd;
            break;
        }
    }

    char message[BUFFER_LEN];
    receive_message(browser_socket_fd, message);

    int session_id = strtol(message, NULL, 10);
    if (session_id == -1) {
        for (int i = 0; i < NUM_SESSIONS; ++i) {

            // Part 3.2
            session_t* session;
            session_ht_t* session_ht_entry;
            HASH_FIND_INT(session_ht, &session_id, session_ht_entry);
            session = malloc(sizeof session);
            session = session_ht_entry->session;

            if (!session->in_use) {
                session_id = i;
                session->in_use = true;
                break;
            }
        }
    }
    browser_list[browser_id].session_id = session_id;

    sprintf(message, "%d", session_id);
    send_message(browser_socket_fd, message);

    return browser_id;
}

/**
 * Handles the given browser by listening to it, processing the message received,
 * broadcasting the update to all browsers with the same session ID, and backing up
 * the session on the disk.
 *
 * @param browser_socket_fd the socket file descriptor of the browser connected
 */
void browser_handler(int browser_socket_fd) {
    int browser_id;

    browser_id = register_browser(browser_socket_fd);

    int socket_fd = browser_list[browser_id].socket_fd;
    int session_id = browser_list[browser_id].session_id;

    printf("Successfully accepted Browser #%d for Session #%d.\n", browser_id, session_id);

    while (true) {
        char message[BUFFER_LEN];
        char response[BUFFER_LEN];

        receive_message(socket_fd, message);
        printf("Received message from Browser #%d for Session #%d: %s\n", browser_id, session_id, message);

        if ((strcmp(message, "EXIT") == 0) || (strcmp(message, "exit") == 0)) {
            close(socket_fd);
            pthread_mutex_lock(&browser_list_mutex);
            browser_list[browser_id].in_use = false;
            pthread_mutex_unlock(&browser_list_mutex);
            printf("Browser #%d exited.\n", browser_id);
            return;
        }

        if (message[0] == '\0') {
            continue;
        }

        bool data_valid = process_message(session_id, message);
        if (!data_valid) {
            // TODO: For Part 3.1, add code here to send the error message to the browser.
            broadcast(session_id, "ERROR: Invalid input!");
        }
        else {
            session_to_str(session_id, response);
            broadcast(session_id, response);
            save_session(session_id);
        }
    }
}

/**
 * Starts the server. Sets up the connection, keeps accepting new browsers,
 * and creates handlers for them.
 *
 * @param port the port that the server is running on
 */
void start_server(int port) {
    // Loads every session if there exists one on the disk.
    load_all_sessions();

    // Creates the socket.
    int server_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_fd == 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Binds the socket.
    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    server_address.sin_port = htons(port);
    if (bind(server_socket_fd, (struct sockaddr *) &server_address, sizeof(server_address)) < 0) {
        perror("Socket bind failed");
        exit(EXIT_FAILURE);
    }

    // Listens to the socket.
    if (listen(server_socket_fd, SOMAXCONN) < 0) {
        perror("Socket listen failed");
        exit(EXIT_FAILURE);
    }
    printf("The server is now listening on port %d.\n", port);

    // Main loop to accept new browsers and creates handlers for them.
    while (true) {
        struct sockaddr_in browser_address;
        socklen_t browser_address_len = sizeof(browser_address);
        int browser_socket_fd = accept(server_socket_fd, (struct sockaddr *) &browser_address, &browser_address_len);
        if ((browser_socket_fd) < 0) {
            perror("Socket accept failed");
            continue;
        }

        // Starts the handler thread for the new browser.
        // TODO: For Part 2.1, creat a thread to run browser_handler() here.
        pthread_t thread;
        int err;

        err = pthread_create(&thread, NULL, (void *(*)(void *)) browser_handler, (void *) browser_socket_fd);
        // err = pthread_create(&thread, NULL, (void *(*)(void *)) browser_handler, & browser_socket_fd);

        if (err) {
            printf("ERROR: Can't create thread: %d\n", err);
            exit(EXIT_FAILURE);
        }

        // pthread_join(thread, NULL);
        // printf("Browser thread joined.\n");
    }

    // Closes the socket.
    close(server_socket_fd);
}

/**
 * The main function for the server.
 *
 * @param argc the number of command-line arguments passed by the user
 * @param argv the array that contains all the arguments
 * @return exit code
 */
int main(int argc, char *argv[]) {
    int port = DEFAULT_PORT;

    if (argc == 1) {
    } else if ((argc == 3)
               && ((strcmp(argv[1], "--port") == 0) || (strcmp(argv[1], "-p") == 0))) {
        port = strtol(argv[2], NULL, 10);

    } else {
        puts("Invalid arguments.");
        exit(EXIT_FAILURE);
    }

    if (port < 1024) {
        puts("Invalid port.");
        exit(EXIT_FAILURE);
    }

    start_server(port);

    exit(EXIT_SUCCESS);
}
