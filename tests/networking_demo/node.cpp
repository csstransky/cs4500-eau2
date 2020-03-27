// Made by Kaylin Devchand and Cristian Stransky

// Client side C/C++ program to demonstrate Socket programming 
#include "../../src/networks/node.h"

const char* DEFAULT_CLIENT_IP = "127.0.0.2";
const char* DEFAULT_SERVER_IP = "127.0.0.1";

// TODO: Super rigid input command getter, but it works and we don't need anything too nice
const char* get_input_client_ip_address(int argc, char const *argv[]) {
    if (argc < 2 || strcmp(argv[1], "-ip") != 0) {
        printf("If you wish to choose an IP for the client, use:\n");
        printf("./client -ip <IP address>\n\n");
        return DEFAULT_CLIENT_IP;
    }
    else {
        return argv[2];
    }
}

// TODO: Super rigid input command getter, but it works and we don't need anything too nice
const char* get_input_server_ip_address(int argc, char const *argv[]) {
    if (argc < 4 || strcmp(argv[3], "-s") != 0) {
        printf("If you wish to choose an IP for the client AND the server, use:\n");
        printf("./client -ip <IP address> -s <Server IP address>\n\n");
        return DEFAULT_SERVER_IP;
    }
    else {
        return argv[4];
    }
}

int main(int argc, char const *argv[]) { 
    srand(time(NULL));
    const char* client_ip_address = get_input_client_ip_address(argc, argv);
    const char* server_ip_address = get_input_server_ip_address(argc, argv);

    Node* node = new Node(client_ip_address, server_ip_address);
    node->connect_to_server(0);

    // Create random timeout between 0 and 10
    // Adding randomness for demo
    int timeout = rand() % 9 + 1;
    node->run_server(timeout);

    // Send message to random node
    String* hi = new String("hi");
    int index = rand() % node->get_num_other_nodes();
    Ack message(node->my_ip_, node->other_nodes_->get(index), hi);
    node->send_message_to_node(&message);
    delete hi;

    timeout = -1;
    node->run_server(timeout);

    delete node;
    return 0;
} 

