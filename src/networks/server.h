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
#include <thread>
#include <mutex>
#include "../helpers/string.h"

const int PORT = 8080;
const int MAX_REQUEST_QUEUE = 3;
const int MAX_CLIENTS = 10;
const int OPT = 1;
const int TIME_OUT = 60;// time out at 60 sec
const int ACK_TIMEOUT = 5;
const char* IP_DEFAULT = "Not registered";

class Server {
    public:
    StringArray* connected_client_ips_; // list of CONNECTED IPs
    int connection_socket_; 
    IntArray* client_sockets_;
    struct sockaddr_in my_address_; 
    String* my_ip_;
    std::thread networking_thread_;
    std::mutex mutex_;

    // Point of this is to store all open fds so we can monitor the open ones
    fd_set readfds_; 

    // Strictly used to test a local KV_Store
    Server() {
        connected_client_ips_ = nullptr;
        client_sockets_ = nullptr;
        my_ip_ = nullptr;
    }

    Server(const char* ip_address) {
        // Create client ip list and sockets
        connected_client_ips_ = new StringArray(MAX_CLIENTS);
        client_sockets_ = new IntArray(MAX_CLIENTS);

        // Creating socket file descriptor
        connection_socket_ = get_new_socket_();

        //set master socket to allow multiple connections ,  
        if(setsockopt(connection_socket_, SOL_SOCKET, SO_REUSEADDR, (char *)&OPT, sizeof(OPT)) < 0) {   
            assert(0);   
        } 

        my_address_ = get_new_sockaddr_(ip_address, PORT); 
        my_ip_ = new String(ip_address);       

        if (bind(connection_socket_, (struct sockaddr *)&my_address_, sizeof(my_address_)) < 0) { 
            printf("\nThe given IP address is not a valid IP address.\n");
            printf("Given IP: %s\n\n", my_ip_->c_str());
            assert(0); 
        } 

        //try to specify maximum of 3 pending connections for the master socket  
        if (listen(connection_socket_, 3) < 0) {   
            assert(0);   
        } 
    }

    ~Server() {  
        // each ip will get freed on shutdown      
        delete connected_client_ips_;
        delete client_sockets_;
        delete my_ip_;
    }

    // Has to be called before server is deleted
    virtual void shutdown() {
        networking_thread_.join(); 
        // close connection socket
        close(connection_socket_);

        // close all sockets
        for (int i = 0; i < client_sockets_->length(); i++) {
            close(client_sockets_->get(i));
            connected_client_ips_->clear();
        }
    }

    int is_not_default_ip_(String* s) {
        return strcmp(s->c_str(), IP_DEFAULT);
    }

    virtual void thread_run_server_(int timeout) {
        timeval timeout_val = {timeout, 0};
        timeval* timeout_pointer = (timeout < 0) ? nullptr : &timeout_val;
        while (wait_for_activty_(timeout_pointer)) {
            check_for_connections_();
            check_for_client_messages_();
        }
        printf("No activity on server.\n");
    }

    // NOTE: -1 runs forever
    void run_server(int timeout) {
        networking_thread_ = std::thread(&Server::thread_run_server_, this, timeout);
    }

    /**
     * Update the list of open file descriptors. Returns the max file descriptor, which is needed
     * for select in wait_activity.
     */
    virtual int set_socket_set_() {
        // clear the socket set  
        FD_ZERO(&readfds_);   
     
        // add connection socket to set  
        FD_SET(connection_socket_, &readfds_);   
        int max_sd = connection_socket_;   
             
        //add child sockets to set  
        for (int i = 0; i < client_sockets_->length(); i++) {   
            //socket descriptor  
            int sd = client_sockets_->get(i);   
                 
            // if valid socket descriptor then add to read list  
            if(sd > 0) { 
                FD_SET( sd , &readfds_);   
            }
                 
            // highest file descriptor number, need it for the select function  
            if(sd > max_sd) { 
                max_sd = sd; 
            }  
        } 

        return max_sd;  
    }

    int wait_for_activty_(timeval* timeout) {
        // Update socket list with active connections, max is the max file descriptor
        int max_sd = set_socket_set_();
        //wait for an activity on one of the sockets , timeout is NULL ,  
        //so wait indefinitely
        // first argument is the highest-numbered file descriptor plus 1 (from man page) 
        return select( max_sd + 1 , &readfds_ , NULL , NULL , timeout); 
    }

    bool is_socket_in_set_(int socket) {
        return FD_ISSET(socket, &readfds_);
    }

    void check_for_connections_() {
        struct sockaddr_in address_client; 
        if (!is_socket_in_set_(connection_socket_)) {
            return;
        }
        
        int addrlen = sizeof(address_client);
        int new_socket;
        if ((new_socket = accept(connection_socket_, (struct sockaddr *)&address_client, (socklen_t*)&(addrlen)))<0)   
        {   
            assert(0);   
        }

        //add new socket to array of sockets
        if (client_sockets_->length() == MAX_CLIENTS) {
            // Add some error handling if there is more than MAX_CLIENTS
            printf("Server can only support %d clients.\n", MAX_CLIENTS);
            assert(0);
        }  

        client_sockets_->push(new_socket);
        String s(IP_DEFAULT);
        connected_client_ips_->push(&s);   
        printf("New Connection. Socket fd is %d, index is %zu\n\n" , new_socket, client_sockets_->length() - 1);  
            
    } 

    int find_ip_in_list_(String* ip) {

        size_t index = connected_client_ips_->index_of(ip);

        if (index == -1) {
            printf("No client with given ip\n");
            assert(0);
        }
        
        return index;
    }

    // returns if the message was handled
    virtual bool decode_message_(Message* message, int client) {
        // common responses to message 
        switch (message->get_kind()) {
            case MsgKind::Ack: {
                printf("Received Ack Message from %s with text ", message->get_sender()->c_str());
                Ack* ack_message = dynamic_cast<Ack*>(message);
                printf("%s\n\n", ack_message->get_message()->c_str());
                break;
            }
            default:
                return 0;
        }
        return 1;
    }

    // nullptr is return if it is a disconnect
    virtual Message* receive_message_(int sd) {
        int message_size; 
        int valread;

        // Check if it was for closing or incomming message
        // read returns 0 if fd was closed
        if ((valread = read(sd, &message_size, sizeof(int))) == 0)   
        {  
            return nullptr;   
                    
        } else { 
            // Read message from client
            char buff[message_size];
            valread = read(sd, &buff, message_size);

            Message* m = deserialize_message(buff);
            return m;
        }   
    }

    void check_for_client_messages_() {
        for (int i = 0; i < client_sockets_->length(); i++) {  
            int sd = client_sockets_->get(i);   
                 
            if (is_socket_in_set_(sd)) {  
                Message* m = receive_message_(sd);
                if (m) {
                    decode_message_(m, i); 
                    delete m;
                } else {
                    remove_client_(i);
                }     
            }   
        }   
    }

    virtual void remove_client_(int index) {
        printf("Client disconnected, ip %s , sd %d, index %d\n\n" ,  
                connected_client_ips_->get(index)->c_str() , client_sockets_->get(index), index);

        // Close the socket and remove from arrays
        close(client_sockets_->remove(index));
        String* removed = connected_client_ips_->remove(index);   
        delete removed;
    }

    int get_new_socket_() {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            printf("\n Socket creation error \n"); 
            assert(0);
        }
        return sock;
    }

    sockaddr_in get_new_sockaddr_(const char* ip_address, int port) {
        struct sockaddr_in sockaddr;
        sockaddr.sin_family = AF_INET; 
        sockaddr.sin_port = htons(port); 
            
        // Convert IPv4 and IPv6 addresses from text to binary form 
        if(inet_pton(AF_INET, ip_address, &sockaddr.sin_addr) <= 0)  { 
            printf("\nERROR: Invalid address/Address not supported \n"); 
            assert(0);
        }
        return sockaddr;
    }

    void send_message(int fd, Message* message) {
        //printf("Sending message with type %d from %s to %s\n\n", message->get_kind(), message->get_sender(), message->get_target());
        char* serial_message = message->serialize();
        int length = message->serial_len();
        
        send(fd, &length, sizeof(int), 0);
        
        send(fd, serial_message, length, 0);

        delete[] serial_message;
    }

    void send_message(String* ip, Message* message) {
        int index = find_ip_in_list_(ip);
        send_message(client_sockets_->get(index), message);
    }

    void send_kill_() {
        for (int i = 0; i < client_sockets_->length(); i++) {
            Message* m = new Kill(my_ip_, connected_client_ips_->get(i));
            printf("Sending kill to sd %d\n\n", client_sockets_->get(i));
            send_message(client_sockets_->get(i), m);
            delete m;
        }
    }

};


