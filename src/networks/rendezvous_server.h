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

    IntArray* node_indexes_;

    RServer(const char* ip_address) : Server(ip_address) {
        node_indexes_ = new IntArray(MAX_CLIENTS);
    }

    ~RServer() {
        delete node_indexes_;
    }
    
    void shutdown() {
        send_kill_();
        Server::shutdown();
        node_indexes_->clear();
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
                // TODO fix this up
                Message* message = new Directory(my_ip_, connected_client_ips_->get(i), &active_clients, &active_node_indexes);
                printf("Sending Directory Message to sd %d\n", client_sockets_->get(i));
                send_message(client_sockets_->get(i), message);
                delete message;
            }
        }
    }

    void decode_message_(Message* message, int client) {
        switch (message->get_kind()) {
            case MsgKind::Register: {
                Register* reg = dynamic_cast<Register*>(message);
                printf("Received Register Message from %s\n", message->get_sender()->c_str());
                String* old = connected_client_ips_->replace(client, message->get_sender());
                while (node_indexes_->length() <= client) {
                    node_indexes_->push(-1);
                }
                node_indexes_->replace(client, reg->get_node_index());
                delete old;
                printf("Updated client ip list with %s\n\n", connected_client_ips_->get(client)->c_str());
                send_directory_message_(); 
                break;
            }
            case MsgKind::Put: {
                printf("Received Put Message from %s with text ", message->get_sender()->c_str());
                Put* put_message = dynamic_cast<Put*>(message);
                printf("%s\n\n", put_message->get_key_name()->c_str());
                break;
            }
            default:
                printf("Received Unknown Message from %s\n\n", message->get_sender()->c_str());
                assert(0);
        }
    }

    void remove_client_(int index) {
        Server::remove_client_(index); 
        node_indexes_->remove(index); 
        
        // Give clients updated list of ips
        send_directory_message_();
    }

};


