#pragma once

#include <condition_variable>

#include "../helpers/map.h"
#include "key.h"
#include "../networks/node.h"

// This is a pseudo value that's used to recognize when an expected value in the get_and_wait_queue_ is 
// actually meant for the local kv_store, and not a remote kv_store.
// NOTE: file descriptor 0 is stdin so this shouldn't be a socket descriptor, choosing other values
// for this variable can cause unexpected behavior (it's possible for a remote kv_store to have a 
// node_index of 6, so making this variable 6 will cause all values to be sent to the remote).
const int LOCAL_SOCKET_DESCRIPTOR = 0;

class KV_Store : public Node {
    public:
    Map* kv_map_; // String* -> Serializer* 
    Map* get_and_wait_queue_; // TODO: refactor to wait_and_get_and_wait_queue_
    size_t local_node_index_;
    std::mutex kv_map_mutex_;
    std::mutex get_and_wait_queue_mutex_; // maps string_key -> array of sockets
    std::condition_variable condition_variable_; // TODO: refactor
    
    KV_Store(const char* client_ip_address, const char* server_ip_address, size_t local_node_index) 
        : Node(client_ip_address, server_ip_address) {
        kv_map_ = new Map();
        get_and_wait_queue_ = new Map();
        local_node_index_ = local_node_index;
    }

    KV_Store(size_t local_node_index) : Node() {
        kv_map_ = new Map();
        get_and_wait_queue_ = new Map();
        local_node_index_ = local_node_index;
    }

    ~KV_Store() {
        delete kv_map_;
        delete get_and_wait_queue_;
    }

    void distribute_value_(IntArray* sockets, Serializer* value) {
        for (int i = 0; i < sockets->length(); i++) {
            int socket = sockets->get(i);
            if (socket > LOCAL_SOCKET_DESCRIPTOR) {
                // TODO: We will need to find a way to actually grab the target IP from the socket
                // and the socket descriptor some time in the future. Low priority though, no needed
                String no_ip("NO TARGET IP");
                Value value_message(my_ip_, &no_ip, value);
                send_message(socket, &value_message);
            } else if (socket == LOCAL_SOCKET_DESCRIPTOR) {
                condition_variable_.notify_one();
            }
        }
    }

    // Puts the key value pair into the map, and also sents the new key value pair to anyone
    // waiting in the queue
    void put_map_(String* key_name, Serializer* value) {
        std::unique_lock<std::mutex> kv_lock(kv_map_mutex_);
        Object* old = kv_map_->put(key_name, value);
        delete old;
        kv_lock.unlock();

        std::unique_lock<std::mutex> get_lock(get_and_wait_queue_mutex_);
        IntArray* sockets = dynamic_cast<IntArray*>(get_and_wait_queue_->remove(key_name));
        get_lock.unlock();
        
        if (sockets) {
            distribute_value_(sockets, value);
            delete sockets;
        }
    }

    Serializer* get_map_(String* key_name) {
        return dynamic_cast<Serializer*>(kv_map_->get(key_name));
    }

    void put_get_and_wait_queue_(String* key_name, int socket) {
        std::unique_lock<std::mutex> lock(get_and_wait_queue_mutex_);
        put_socket_into_queue_(key_name, socket);
    }

    void put(Key* key, Object* value) {
        Serializer serial(value->serial_len());
        serial.serialize_object(value);
        if (key->get_node_index() == local_node_index_) {
            put_map_(key->get_key(), &serial);
        } 
        else {
            // call upon another Node to put the kv
            int index = other_node_indexes_->index_of(key->get_node_index());
            Put message(my_ip_, other_nodes_->get(index), key->get_key(), &serial);
            send_message_to_node(&message);
        }
    }

    size_t get_node_index() {
        return local_node_index_;
    }

    // NOTE: message should be either a Get or WaitAndGet
    char* send_message_and_receive_serial_(Message& message) {
        Value* value_message = dynamic_cast<Value*>(send_message_to_node_wait(&message));
        char* serial = value_message->get_serial();
        delete value_message;
        return serial;
    }

    // Returns a new char array, make sure to delete it later
    char* get_value_serial(Key* key) {
        if (key->get_node_index() == local_node_index_) {
            Serializer* map_serial = get_map_(key->get_key());
            return map_serial->get_serial();
        }
        else {
            int index = other_node_indexes_->index_of(key->get_node_index());
            Get message(my_ip_, other_nodes_->get(index), key->get_key());
            return send_message_and_receive_serial_(message);
        }

    }

    void put_socket_into_queue_(String* key_name, int socket_descriptor) {
        IntArray* sockets = dynamic_cast<IntArray*>(get_and_wait_queue_->get(key_name));
        if (sockets) {
            sockets->push(socket_descriptor);
        } else {
            IntArray int_array(1);
            int_array.push(socket_descriptor);
            get_and_wait_queue_->put(key_name, &int_array);
        }
    }

    Serializer* wait_for_local_map_value_(Key* key) {
        put_socket_into_queue_(key->get_key(), LOCAL_SOCKET_DESCRIPTOR);
        std::unique_lock<std::mutex> lock(kv_map_mutex_);
        condition_variable_.wait(lock);
        lock.unlock();
        return get_map_(key->get_key());
    }

    char* wait_get_value_serial(Key* key) {
        if (key->get_node_index() == local_node_index_) {
            Serializer* map_serial = get_map_(key->get_key());

            if (!map_serial) {
                map_serial = wait_for_local_map_value_(key); 
            } 
            return map_serial->get_serial();
        }
        else {
            int index = other_node_indexes_->index_of(key->get_node_index());
            WaitAndGet message(my_ip_, other_nodes_->get(index), key->get_key());
            return send_message_and_receive_serial_(message);
        }

    }

    Array* get_array(Key* key, char type) {
        char* kv_serial = get_value_serial(key);
        Deserializer deserializer(kv_serial);
        Array* array;
        switch(type) {
            case 'I': array = new IntArray(deserializer); break;
            case 'B': array = new BoolArray(deserializer); break;
            case 'D': array = new DoubleArray(deserializer); break;
            case 'S': array = new StringArray(deserializer); break;
        }
        delete[] kv_serial;
        return array;
    }

    size_t get_node_index(size_t index) {
        return other_node_indexes_ ? other_node_indexes_->get(index) : local_node_index_;
    }

    bool decode_message_(Message* message, int client) {
        switch (message->get_kind()) {
            case MsgKind::Put: {
                Put* put_message = dynamic_cast<Put*>(message);
                put_map_(put_message->get_key_name(), put_message->get_value());
                return 1;
            }
            case MsgKind::Get: {
                Get* get_message = dynamic_cast<Get*>(message);
                Serializer* value = get_map_(get_message->get_key_name());
                if (!value) {
                    // There is no key value pair, for the given key
                    assert(0);
                }
                Value value_message(my_ip_, get_message->get_sender(), value);
                send_message(client_sockets_->get(client), &value_message);
                return 1;
            }
            case MsgKind::WaitAndGet: {
                WaitAndGet* get_message = dynamic_cast<WaitAndGet*>(message);
                Serializer* value = get_map_(get_message->get_key_name());
                if (value) {
                    Value value_message(my_ip_, get_message->get_sender(), value);
                    send_message(client_sockets_->get(client), &value_message);
                } else {
                    put_get_and_wait_queue_(get_message->get_key_name(), client_sockets_->get(client));
                }
                return 1;
            }   
            default:
                // Priority is now kicked up to the Parent class
                return Node::decode_message_(message, client);
        }
    }
};
