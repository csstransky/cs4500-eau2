#include <sys/wait.h>

#include "../src/networks/node.h"
#include "../src/networks/rendezvous_server.h"

void test_connection() {
    int cpid;
    String server_ip("127.0.0.1");
    String client_ip("127.0.0.2");

    // Fork to create another process
    if ((cpid = fork())) {
        // In parent process

        // Start server
        RServer* server = new RServer(server_ip.c_str()); 
        server->run_server(2);

        // check for registered client
        assert(server->client_sockets_->length() == 1);
        assert(server->connected_client_ips_->length() == 1);
        assert(server->connected_client_ips_->get(0)->equals(&client_ip));
        assert(server->node_indexes_->length() == 1);
        assert(server->node_indexes_->get(0) == 0);
        
        // wait for child to finish
        int st;
        waitpid(cpid, &st, 0);
        server->run_server(0.5);
        server->shutdown();
        delete server;
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        Node* node = new Node(client_ip.c_str(), server_ip.c_str());
        node->connect_to_server(0);
        node->run_server(2);

        // check for list of nodes
        assert(node->other_nodes_->length() == 1);
        assert(node->other_nodes_->get(0)->equals(&client_ip));
        assert(node->other_node_indexes_->length() == 1);
        assert(node->other_node_indexes_->get(0) == 0);

        node->shutdown();
        delete node;

        // exit
        exit(0);
    }

    printf("Networking registration test passed!\n");


}

int main(int argc, char** argv) {
    test_connection();
    printf("All networking tests pass!\n");
    return 0;
}