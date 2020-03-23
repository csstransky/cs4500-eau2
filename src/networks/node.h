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
    StringArray* other_nodes_; // TODO: Get StringArray working
    IntArray* kv_indexes_; // TODO: get this guy set up
    size_t num_other_nodes_;
    bool kill_;

    Node(const char* client_ip_address, const char* server_ip_address) : Server(client_ip_address) {
        server_address_ = get_new_sockaddr_(server_ip_address, PORT);  
        server_socket_ = 0; 
        server_ip_ = new String(server_ip_address);  
        kill_ = false;  
        num_other_nodes_ = 0;
        other_nodes_ = new String*[num_other_nodes_]; 
    }

    ~Node() {
 
        delete server_ip_;
        if (other_nodes_) {
            delete other_nodes_;
        }
    }

    void shutdown() {
        if (server_socket_) {
            close(server_socket_);
        }

        Server::shutdown();
    }

    // NOTE: timeout -1 runs forever
    void run_server(int timeout) {
        timeval timeout_val = {timeout, 0};
        timeval* timeout_pointer = (timeout < 0) ? nullptr : &timeout_val;
        while (wait_for_activty_(timeout_pointer) && !kill_) {
            check_for_connections_();
            check_for_client_messages_();
            check_server_messages_();
        }
    }

    void register_with_server_(size_t local_node_index) {
        // TODO add node index here
        Message* m = new Register(my_ip_, server_ip_, local_node_index);
        printf("Sending ip\n");
        send_message(server_socket_, m);
        printf("\n");
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
    void decode_message_(Message* message, int client) {
        switch (message->get_kind()) {
            case MsgKind::Put: {
                Put* put_message = dynamic_cast<Put*>(message);
                printf("Message from %s, text: %s\n\n", message->get_sender()->c_str(), put_message->get_message()->c_str());
                break;
            }
            case MsgKind::Directory: {
                printf("Received Directory Message\n\n");
                Directory* dir_message = dynamic_cast<Directory*>(message);
                delete other_nodes_;
                other_nodes_ = dir_message->get_addresses();
                delete kv_indexes_;
                kv_indexes_ = dir_message->get_kv_indexes();
                local_kv_index_ = dir_message->get_local_kv_index();
                break;
            }
            case MsgKind::Kill: {
                printf("Received Kill message\n\n");
                kill_ = true;
                shutdown();
                break;
            }
            case MsgKind::Register: 
                printf("Received Register Message from %s\n\n", message->get_sender()->c_str());
                break;
            default:
                assert(0);
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
        return num_other_nodes_;
    }

    void send_message_to_node(String* node_ip, Message* message) {
        // Create socket and connect to node
        int socket = get_new_socket_();
        sockaddr_in address = get_new_sockaddr_(node_ip->c_str(), PORT);
        if (connect(socket, (struct sockaddr *)&address, sizeof(address)) < 0) { 
            printf("\nConnection Failed \n"); 
            assert(0);
        }

        // Send message
        send_message(socket, message);

        // Close connections
        close(socket);
    }

    void send_put_message_to_node(String* message, int index) {        

        // Create message
        Message* m = new Put(my_ip_, other_nodes_[index], message);

        send_message_to_node(other_nodes_[index], m);
        delete m;
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
                // TODO: In the future, add a client reconnect attempt method
                assert(0);
            }
            
        }
    }
};


