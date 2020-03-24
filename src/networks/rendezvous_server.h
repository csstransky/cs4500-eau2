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
        node_indexes_ = new int[MAX_CLIENTS]; // TODO, actually set this up correctly
    }

    ~RServer() {}
    
    void shutdown() {
        send_kill_();
        Server::shutdown();
    }

    void send_directory_message_() {
        // Create list of active client ips
        int count = 0;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (connected_client_ips_[i]) {
                count += 1;
            }
        }

        String** active_clients = new String*[count];

        int index = 0;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            // Add all registered ips
            if (connected_client_ips_[i] && is_not_default_ip_(connected_client_ips_[i])) {
                active_clients[index] = connected_client_ips_[i];
                index++;
            }
        }

        for (int i = 0; i < MAX_CLIENTS; i++) {
            // if socket is has registered send the message
            if (connected_client_ips_[i] && is_not_default_ip_(connected_client_ips_[i])) {
                // TODO fix this up
                Message* message = new Directory(my_ip_, connected_client_ips_[i], active_clients, count);
                //Message* message = new Register(my_ip_, connected_client_ips_[i]);
                printf("Sending Directory Message to sd %d\n", client_sockets_[i]);
                send_message(client_sockets_[i], message);
                delete message;
            }
        }

        delete[] active_clients;
    }

    void decode_message_(Message* message, int client) {
        switch (message->get_kind()) {
            case MsgKind::Register: {
                printf("Received Register Message from %s\n", message->get_sender()->c_str());
                delete connected_client_ips_[client];
                connected_client_ips_[client] = message->get_sender()->clone();
                printf("Updated client ip list with %s\n\n", connected_client_ips_[client]->c_str());
                send_directory_message_(); 
                break;
            }
            case MsgKind::Put: {
                printf("Received Put Message from %s with text ", message->get_sender()->c_str());
                Put* put_message = dynamic_cast<Put*>(message);
                printf("%s\n\n", put_message->get_message()->c_str());
                break;
            }
            default:
                printf("Received Unknown Message from %s\n\n", message->get_sender()->c_str());
                assert(0);
        }
    }

    void remove_client_(int index) {
        Server::remove_client_(index);  
        
        // Give clients updated list of ips
        send_directory_message_();
    }

};


