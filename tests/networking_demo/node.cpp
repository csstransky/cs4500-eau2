// Made by Kaylin Devchand and Cristian Stransky

// Client side C/C++ program to demonstrate Socket programming 
#include "../../src/networks/node.h"
#include "../../src/application/arguments.h"

int main(int argc, char const *argv[]) { 
    srand(time(NULL));
    const char* client_ip_address = get_input_client_ip_address(argc, argv);
    const char* server_ip_address = get_input_server_ip_address(argc, argv);

    Node* node = new Node(client_ip_address, server_ip_address);
    node->connect_to_server(0);

    node->run_server(2);
    sleep(1);

    // Send message to random node
    String* hi = new String("hi");
    int index = rand() % node->get_num_other_nodes();
    printf("index: %d\n", index);
    Ack message(node->my_ip_, node->other_nodes_->get(index), hi);
    node->send_message_to_node(&message);
    node->wait_for_shutdown();
    delete hi;

    delete node;
    return 0;
} 

