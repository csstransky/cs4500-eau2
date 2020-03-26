#include <sys/wait.h>

#include "../src/networks/node.h"
#include "../src/networks/rendezvous_server.h"

void test_registration() {
    int cpid;
    String* server_ip = new String("127.0.0.1");
    String* client_ip = new String("127.0.0.2");

    // Fork to create another process
    if ((cpid = fork())) {
        // In parent process

        // Start server
        RServer* server = new RServer(server_ip->c_str()); 
        server->run_server(2);

        // check for registered client
        assert(server->client_sockets_->length() == 1);
        assert(server->connected_client_ips_->length() == 1);
        assert(server->connected_client_ips_->get(0)->equals(client_ip));
        assert(server->node_indexes_->length() == 1);
        assert(server->node_indexes_->get(0) == 0);
        
        // wait for child to finish
        int st;
        waitpid(cpid, &st, 0);
        server->run_server(0.5);
        server->shutdown();
        delete server;
        delete client_ip;
        delete server_ip;
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        Node* node = new Node(client_ip->c_str(), server_ip->c_str());
        node->connect_to_server(0);
        node->run_server(5);

        // check for list of nodes
        assert(node->other_nodes_->length() == 1);
        assert(node->other_nodes_->get(0)->equals(client_ip));
        assert(node->other_node_indexes_->length() == 1);
        assert(node->other_node_indexes_->get(0) == 0);

        node->shutdown();
        delete node;
        delete server_ip;
        delete client_ip;

        // exit
        exit(0);
    }

    printf("Networking registration test passed!\n");
}

void test_kill() {
    int cpid;
    String* server_ip = new String("127.0.0.1");
    String* client_ip = new String("127.0.0.2");

    // Fork to create another process
    if ((cpid = fork())) {
        // In parent process

        // Start server
        RServer* server = new RServer(server_ip->c_str()); 
        server->run_server(2);

        // check for registered client
        assert(server->client_sockets_->length() == 1);
        assert(server->connected_client_ips_->length() == 1);
        assert(server->connected_client_ips_->get(0)->equals(client_ip));
        assert(server->node_indexes_->length() == 1);
        assert(server->node_indexes_->get(0) == 0);

        server->run_server(0.5);
        server->shutdown();
        
        // wait for child to finish
        int st;
        waitpid(cpid, &st, 0);
        delete server;
        delete client_ip;
        delete server_ip;
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        Node* node = new Node(client_ip->c_str(), server_ip->c_str());
        node->connect_to_server(0);
        node->run_server(-1);

        assert(node->kill_ == true);

        delete node;
        delete server_ip;
        delete client_ip;

        // exit
        exit(0);
    }

    printf("Networking kill message test passed!\n");
}

void test_multiple_nodes() {
    int cpid[3];
    String* server_ip = new String("127.0.0.1");
    String** client_ips = new String*[3];
    client_ips[0] = new String("127.0.0.2");
    client_ips[1] = new String("127.0.0.3");
    client_ips[2] = new String("127.0.0.4");

    // In parent process

    // Start server
    RServer* server = new RServer(server_ip->c_str()); 

    for (int i = 0; i < 3; i++) {
        if ((cpid[i] = fork())) {
            // do nothing now
        } else {
            // In child process

            // start node
            Node* node = new Node(client_ips[i]->c_str(), server_ip->c_str());
            node->connect_to_server(0);
            node->run_server(2);

            // check for list of nodes
            assert(node->other_nodes_->length() == 3);
            assert(node->other_node_indexes_->length() == 3);
            printf("Node %d passed asserts\n", i);

            node->run_server(-1);
            delete node;
            delete server_ip;
            for (int i = 0; i < 3; i++) {
                delete client_ips[i];
            }
            delete[] client_ips;
            delete server;

            // exit
            exit(0);
        }
    }
    server->run_server(3);


    // check for registered client
    assert(server->client_sockets_->length() == 3);
    assert(server->connected_client_ips_->length() == 3);
    assert(server->node_indexes_->length() == 3);
    printf("Parent passed asserts\n");
    server->run_server(0.5);
    server->shutdown();
    
    // wait for children to finish
    for (int i = 0; i < 3; i++) {
        int st;
        waitpid(cpid[i], &st, 0);
    }

    delete server;
    for (int i = 0; i < 3; i++) {
        delete client_ips[i];
    }
    delete[] client_ips;
    delete server_ip;

    printf("Networking multiple nodes test passed!\n");
}


int main(int argc, char** argv) {
    test_registration();
    test_kill();
    test_multiple_nodes();
    printf("All networking tests pass!\n");
    return 0;
}