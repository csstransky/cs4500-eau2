// Made by Kaylin Devchand and Cristian Stransky
// Geeks for geeks was used as a guide.
//https://www.geeksforgeeks.org/socket-programming-in-cc-handling-multiple-clients-on-server-without-multi-threading/

#pragma once
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

class RServer : public Server {
    public:
    // keeps track of how many Nodes are done with their Application
    size_t node_complete_count_; 
    IntArray* node_indexes_;

    RServer(const char* ip_address) : Server(ip_address) {
        node_indexes_ = new IntArray(MAX_CLIENTS);
        node_complete_count_ = 0;
    }

    ~RServer() {
        delete node_indexes_;
    }

    // Checks to see if all Nodes connected to this RServer are complete with their Application.
    bool are_nodes_complete() {
        return node_complete_count_ > 0 && node_complete_count_ >= node_indexes_->length();
    }

    void thread_run_server_(int timeout) {
        while (!are_nodes_complete() && wait_for_activity_(timeout)) {
            check_for_connections_();
            check_for_client_messages_();
        }
    }
    
    // joins the thread
    void wait_for_shutdown() {
        networking_thread_.join(); 
        send_kill_();

        node_indexes_->clear();
        // close connection socket
        close(connection_socket_);

        // close all sockets
        for (int i = 0; i < client_sockets_->length(); i++) {
            close(client_sockets_->get(i));
            connected_client_ips_->clear();
        }
    }

    void send_directory_message_() {
        StringArray active_clients(MAX_CLIENTS);
        IntArray active_node_indexes(MAX_CLIENTS);

        for (int i = 0; i < connected_client_ips_->length(); i++) {
            if (is_not_default_ip_(connected_client_ips_->get(i))) {
                active_clients.push(connected_client_ips_->get(i));
                active_node_indexes.push(node_indexes_->get(i));
            }
        }

        for (int i = 0; i < connected_client_ips_->length(); i++) {
            // if socket is has registered send the message
            if (is_not_default_ip_(connected_client_ips_->get(i))) {
                Message* message = new Directory(my_ip_, connected_client_ips_->get(i), &active_clients, &active_node_indexes);
                send_message(client_sockets_->get(i), message);
                delete message;
            }
        }
    }

    bool decode_message_(Message* message, int client) {
        switch (message->get_kind()) {
            case MsgKind::Register: {
                Register* reg = dynamic_cast<Register*>(message);
                String* old = connected_client_ips_->replace(client, message->get_sender());
                while (node_indexes_->length() <= client) {
                    node_indexes_->push(-1);
                }
                node_indexes_->replace(client, reg->get_node_index());
                delete old;
                send_directory_message_(); 
                return 1;
            }
            case MsgKind::Complete: {
                node_complete_count_++;
                return 1;
            }
            default:
                // Nobody inherits from rserver so it has to handle the message
                return Server::decode_message_(message, client);
        }
    }

    void remove_client_(int index) {
        Server::remove_client_(index); 
        node_indexes_->remove(index); 
        
        // Give clients updated list of ips
        send_directory_message_();
    }

};


