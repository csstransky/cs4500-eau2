// Made by Kaylin Devchand and Cristian Stransky

#include "../../src/networks/rendezvous_server.h"
#include "../../src/application/arguments.h"

int main(int argc, char const *argv[]) 
{     
    const char* ip_address = get_input_client_ip_address(argc, argv);
    
    RServer* server = new RServer(ip_address); 
    server->run_server();
    server->wait_for_shutdown();
    delete server;
    return 0;
} 