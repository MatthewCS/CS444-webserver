all: server browser

server: server.c net_util.h net_util.c
	gcc -std=c11 server.c net_util.c -o server -pthread

browser: browser.c net_util.h net_util.c
	gcc -std=c11 browser.c net_util.c -o browser -pthread
clean:
	rm -f *.o server browser
