// Made by Kaylin Devchand and Cristian Stransky

#include "../../src/networks/rendezvous_server.h"
#include "arguments.h"

int TIMEOUT = 60 * 30; // 30 minutes might actually be a tad too much

int main(int argc, char const *argv[]) 
{     
    const char* ip_address = get_input_client_ip_address(argc, argv);
    RServer* server = new RServer(ip_address); 
    server->run_server(TIMEOUT);
    server->wait_for_shutdown();
    delete server;
    return 0;
} 