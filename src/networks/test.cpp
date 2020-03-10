// Made by Kaylin Devchand and Cristian Stransky

#include "../src/node.h"
#include "../src/rendezvous_server.h"

int main(int argc, char const *argv[]) 
{ 
    const char* server_ip = "127.0.0.1";
    const char* client_ip = "127.0.0.3";
    RServer* server = new RServer(server_ip);
    Node* client = new Node(client_ip, server_ip);

    int timeout = 3;
    String server_ip_string(server_ip);
    String client_ip_string(client_ip);
    server->run_server(timeout);
    client->run_server(timeout);
    client->connect_to_server();
    client->send_message(&server_ip_string, new Ack(&client_ip_string, &server_ip_string));
    server->run_server(timeout);
    client->shutdown();
    server->shutdown();

    return 0;
} 