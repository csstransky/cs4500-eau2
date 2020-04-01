#pragma once

#include <condition_variable>

#include "../helpers/map.h"
#include "key.h"
#include "../networks/node.h"

// This is a pseudo value that's used to recognize when an expected value in the get_queue_ is 
// actually meant for the local kv_store, and not a remote kv_store.
// NOTE: file descriptor 0 is stdin so this shouldn't be a socket descriptor, choosing other values
// for this variable can cause unexpected behavior (it's possible for a remote kv_store to have a 
// node_index of 6, so making this variable 6 will cause all values to be sent to the remote).
const int LOCAL_SOCKET_DESCRIPTOR = 0;

class KV_Store : public Node {
    public:
    SOMap* kv_map_; // String* -> Serializer* 
    SIAMap* get_queue_;
    size_t local_node_index_;
    std::mutex kv_map_mutex_;
    std::mutex get_queue_mutex_;
    std::condition_variable cv_;
    
    KV_Store(const char* client_ip_address, const char* server_ip_address, size_t local_node_index) 
        : Node(client_ip_address, server_ip_address) {
        kv_map_ = new SOMap();
        get_queue_ = new SIAMap();
        local_node_index_ = local_node_index;
    }

    KV_Store(size_t local_node_index) : Node() {
        kv_map_ = new SOMap();
        get_queue_ = new SIAMap();
        local_node_index_ = local_node_index;
    }

    ~KV_Store() {
        delete kv_map_;
        delete get_queue_;
    }

    void distribute_value_(IntArray* sockets, Serializer* value) {
        for (int i = 0; i < sockets->length(); i++) {
            int socket = sockets->get(i);
            if (socket > LOCAL_SOCKET_DESCRIPTOR) {
                // TODO: Target IPs will be removed in the future
                String no_ip("NO TARGET IP");
                Value value_message(my_ip_, &no_ip, value);
                send_message(socket, &value_message);
            } else if (socket == LOCAL_SOCKET_DESCRIPTOR) {
                cv_.notify_one();
            }
        }
    }

    // Puts the key value pair into the map, and also sents the new key value pair to anyone
    // waiting in the queue
    void put_map_(String* key_name, Serializer* value) {
        std::unique_lock<std::mutex> kv_lock(kv_map_mutex_);
        kv_map_->put(key_name, value);
        kv_lock.unlock();

        std::unique_lock<std::mutex> get_lock(get_queue_mutex_);
        IntArray* sockets = dynamic_cast<IntArray*>(get_queue_->remove(key_name));
        get_lock.unlock();
        
        if (sockets) {
            distribute_value_(sockets, value);
            delete sockets;
        }
    }

    Serializer* get_map_(String* key_name) {
        return dynamic_cast<Serializer*>(kv_map_->get(key_name));
    }

    void put_get_queue_(String* key_name, int socket) {
        std::unique_lock<std::mutex> lock(get_queue_mutex_);
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
        IntArray* sockets = get_queue_->get(key_name);
        if (sockets) {
            sockets->push(socket_descriptor);
        } else {
            IntArray int_array(1);
            int_array.push(socket_descriptor);
            get_queue_->put(key_name, &int_array);
        }
    }

    Serializer* wait_for_local_map_value_(Key* key) {
        put_socket_into_queue_(key->get_key(), LOCAL_SOCKET_DESCRIPTOR);
        std::unique_lock<std::mutex> lock(kv_map_mutex_);
        cv_.wait(lock);
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

    Array* get_array(Key* key) {
        char* kv_serial = get_value_serial(key);
        Array* array = new Array(kv_serial);
        delete[] kv_serial;
        return array;
    }

    // Returns a new IntArray, make sure to delete it later
    IntArray* get_int_array(Key* key) {
        char* kv_serial = get_value_serial(key);
        IntArray* int_array = IntArray::deserialize(kv_serial);
        delete[] kv_serial;
        return int_array;
    }

    // Returns a new BoolArray, make sure to delete it later
    BoolArray* get_bool_array(Key* key) {
        char* kv_serial = get_value_serial(key);
        BoolArray* bool_array = BoolArray::deserialize(kv_serial);
        delete[] kv_serial;
        return bool_array;
    }

    // Returns a new DoubleArray, make sure to delete it later
    DoubleArray* get_double_array(Key* key) {
        char* kv_serial = get_value_serial(key);
        DoubleArray* double_array = DoubleArray::deserialize(kv_serial);
        delete[] kv_serial;
        return double_array;
    }

    // Returns a new StringArray, make sure to delete it later
    StringArray* get_string_array(Key* key) {
        char* kv_serial = get_value_serial(key);
        StringArray* string_array = StringArray::deserialize(kv_serial);
        delete[] kv_serial;
        return string_array;
    }

    size_t get_node_index(size_t index) {
        return other_node_indexes_ ? other_node_indexes_->get(index) : local_node_index_;
    }

    bool decode_message_(Message* message, int client) {
        if (Node::decode_message_(message, client)) {
            return 1;
        }
        switch (message->get_kind()) {
            case MsgKind::Put: {
                Put* put_message = dynamic_cast<Put*>(message);
                put_map_(put_message->get_key_name(), put_message->get_value());
                break;
            }
            case MsgKind::Get: {
                Get* get_message = dynamic_cast<Get*>(message);
                Serializer* value = get_map_(get_message->get_key_name());
                if (!value) {
                    // There is no key value pair, for the given key
                    assert(0);
                    // TODO: think of a better way to do this than an assert, maybe send an Nack message instead
                }
                Value value_message(my_ip_, get_message->get_sender(), value);
                send_message(client_sockets_->get(client), &value_message);
                break;
            }
            case MsgKind::WaitAndGet: {
                WaitAndGet* get_message = dynamic_cast<WaitAndGet*>(message);
                Serializer* value = get_map_(get_message->get_key_name());
                if (value) {
                    Value value_message(my_ip_, get_message->get_sender(), value);
                    send_message(client_sockets_->get(client), &value_message);
                } else {
                    put_get_queue_(get_message->get_key_name(), client_sockets_->get(client));
                }
                break;
            }   
            default:
                // Nobody inherits from kv store so it has to handle the message
                return 0;
        }
        return 1;
    }
};
