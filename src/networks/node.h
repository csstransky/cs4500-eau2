// Made by Kaylin Devchand and Cristian Stransky

#pragma once

// Server side C/C++ program to demonstrate Socket programming 
#include <unistd.h> 
#include <stdio.h> 
#include <sys/socket.h> 
#include <stdlib.h> 
#include <netinet/in.h> 
#include <string.h> 
#include <arpa/inet.h> 
#include "message.h"
#include <errno.h>
#include <assert.h>
#include "../helpers/string.h"
#include "server.h"

class Node : public Server {
    public:
    // Socket to communicate with server
    int server_socket_;
    // Address of server
    struct sockaddr_in server_address_;
    String* server_ip_;
    // list of POSSIBLE IPs to connect to
    StringArray* other_nodes_; 
    IntArray* other_node_indexes_;
    bool kill_;

    // Strictly used to test a local KV_Store
    Node() : Server(){
        server_ip_ = nullptr;
        other_nodes_ = nullptr;
        other_node_indexes_ = nullptr;
    }

    Node(const char* client_ip_address, const char* server_ip_address) : Server(client_ip_address) {
        server_address_ = get_new_sockaddr_(server_ip_address, PORT);  
        server_socket_ = 0; 
        server_ip_ = new String(server_ip_address);  
        kill_ = false;  
        other_nodes_ = nullptr;
        other_node_indexes_ = nullptr; 
    }

    ~Node() {
        delete server_ip_;
        delete other_nodes_;
        delete other_node_indexes_;
    }

    // Node needs to tell the RServer that it's complete with its task, so that this Node is ready
    // to shutdown.
    // When all Nodes are complete, they will receive a Kill message to shutdown.
    void signal_complete_to_server_() {
        Message* m = new Complete(my_ip_, server_ip_);
        send_message(server_socket_, m);
        delete m;
    }

    void wait_for_shutdown() {
        signal_complete_to_server_();
        Server::wait_for_shutdown();
        if (server_socket_) {
            close(server_socket_);
        }
    }

    // NOTE: timeout -1 runs forever
    void thread_run_server_(int timeout) {
        while (wait_for_activity_(timeout) && !kill_) {
            check_for_connections_();
            check_for_client_messages_();
            check_server_messages_();
        }
    }

    void register_with_server_(size_t local_node_index) {
        Message* m = new Register(my_ip_, server_ip_, local_node_index);
        send_message(server_socket_, m);
        delete m;
    
    }

    void connect_to_server(size_t local_node_index) {
        server_socket_ = get_new_socket_();
        if (connect(server_socket_, (struct sockaddr *)&server_address_, sizeof(server_address_)) < 0) { 
            printf("\nConnection Failed \n"); 
            assert(0);
        }
        register_with_server_(local_node_index);
    }

    // -1 is the server
    bool decode_message_(Message* message, int client) {
        switch (message->get_kind()) {
            case MsgKind::Directory: {
                Directory* dir_message = dynamic_cast<Directory*>(message);
                delete other_nodes_;
                other_nodes_ = dir_message->get_addresses()->clone();
                delete other_node_indexes_;
                other_node_indexes_ = dir_message->get_node_indexes()->clone();
                return 1;
            }
            case MsgKind::Kill: {
                kill_ = true;
                return 1;
            }
            default:
                // Priority is now kicked up to the Parent class
                return Server::decode_message_(message, client);
        }
    }

    int set_socket_set_() {
        int max = Server::set_socket_set_();   
     
        //add server socket to set  
        FD_SET(server_socket_, &readfds_);

        if (server_socket_ > max) {
            return server_socket_;
        } else {
            return max;
        }
    }

    int get_num_other_nodes() {
        return other_node_indexes_? other_node_indexes_->length() : 1;
    }

    void send_message_to_node(Message* message) {
        String* node_ip = message->get_target();
        // Create socket and connect to node
        int socket = get_new_socket_();
        sockaddr_in address = get_new_sockaddr_(node_ip->c_str(), PORT);
        if (connect(socket, (struct sockaddr *)&address, sizeof(address)) < 0) { 
            printf("\nConnection Failed \n"); 
            assert(0);
        }

        send_message(socket, message);

        // Close connections
        int rv = close(socket);
        assert(rv == 0);
    }

    // sends a message to a node and waits for a message back from that node
    Message* send_message_to_node_wait(Message* message) {
        String* node_ip = message->get_target();
        // Create socket and connect to node
        int socket = get_new_socket_();
        sockaddr_in address = get_new_sockaddr_(node_ip->c_str(), PORT);
        if (connect(socket, (struct sockaddr *)&address, sizeof(address)) < 0) { 
            printf("\nConnection Failed \n"); 
            assert(0);
        }

        send_message(socket, message);

        Message* m = receive_message_(socket);

        // Close connections
        close(socket);

        return m;
    }

    void check_server_messages_() {
        if (FD_ISSET(server_socket_, &readfds_)) {
            // Read message from server
            Message* m = receive_message_(server_socket_);

            if (m) {
                decode_message_(m, -1);
                delete m;
            } else {
                printf("Server disconnected\n");
                assert(0);
            }
            
        }
    }
};


