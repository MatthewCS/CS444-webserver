# CS 444/544 Project: Prototyping a Web Server/Browser

## Server

### Capacity

- Number of sessions: 128
- Number of browsers: 128
- Number of variables per session: 26

### Data Structure

- `session_struct`: Stores the information of a session.
- `browser_struct`: Stores the information of a browser.

### Global Static Variables

- `browser_t browser_list[NUM_BROWSER]`: Stores the information of all browsers.
- `session_t session_list[NUM_SESSIONS]`: Stores the information of all sessions.
- `pthread_mutex_t browser_list_mutex`: A mutex lock for the browser list.
- `pthread_mutex_t session_list_mutex`: A mutex lock for the session list.

### Functions

- `void session_to_str(int session_id, char result[])`: Returns the string format of the given session.
- `bool is_str_numeric(const char str[]);`: Determines if the given string represents a number.
- `bool process_message(int session_id, const char message[])`: Process the given message and update the given session if it is valid.
- `void broadcast(int session_id, const char message[])`: Broadcasts the given message to all browsers with the same session ID.
- `void get_session_file_path(int session_id, char path[])`: Gets the path for the given session.
- `void load_all_sessions()`: Loads every session from the disk one by one if it exists.
- `void save_session(int session_id)`: Saves the given sessions to the disk.
- `int register_browser(int browser_socket_fd)`: Assigns a browser ID to the new browser. Determines the correct session ID for the new browser through the interaction with it.
- `void browser_handler(int browser_socket_fd)`: Handle the given browser.
- `void start_server(int port) `: Starts the server.

## Browser

### Static Variables

- `browser_on`: Determines if the browser is on/off.
- `server_socket_fd`: The socket file descriptor of the server that is currently being connected.
- `session_id`: The session ID of the session on the server that is currently being accessed.

### Functions

- `void read_user_input(char message[])`: Reads the user input from stdin.
- `void load_cookie()`: Loads the cookie from the disk and gets the session ID if there exists one. Otherwise, assigns the session ID to be -1.
- `void save_cookie()`: Saves the session ID to the cookie on the disk.
- `void register_server()`: Interacts with the server to get or confirm the final session ID.
- `void server_listener()`: Listens to the server.
- `void start_browser(const char host_ip[], int port);`: Starts the browser.

## Network Utility

### Default Settings

- Host IP: 127.0.0.1
- Port: 7000
- Buffer length: 1024

### Functions

- `ssize_t send_message(int socket_fd, const char message[])`: Sends the message through socket.
- `ssize_t receive_message(int socket_fd, char message[])`: Receives the message through socket.
