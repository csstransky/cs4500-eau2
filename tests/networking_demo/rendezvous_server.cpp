// Made by Kaylin Devchand and Cristian Stransky

#include "../../src/networks/rendezvous_server.h"

const char* DEFAULT_SERVER_IP = "127.0.0.1";

const char* get_input_ip_address(int argc, char const *argv[]) {
    if (argc < 2 || strcmp(argv[1], "-ip") != 0) {
        printf("If you wish to choose an IP for the server, use:\n");
        printf("./server -ip <IP address>\n\n");
        return DEFAULT_SERVER_IP;
    }
    else {
        return argv[2];
    }
}

int main(int argc, char const *argv[]) 
{     
    const char* ip_address = get_input_ip_address(argc, argv);
    
    RServer* server = new RServer(ip_address); 
    server->run_server(TIME_OUT);
    server->shutdown();
    delete server;
    return 0;
} 