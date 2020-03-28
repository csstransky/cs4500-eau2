#pragma once

#include <condition_variable>

#include "../helpers/map.h"
#include "key.h"
#include "../networks/node.h"

class KV_Store : public Node {
    public:
    SOMap* kv_map_; // String* -> Serializer* 
    SIMap* get_queue_;
    size_t local_node_index_;
    std::mutex kv_map_mutex_;
    std::mutex get_queue_mutex_;
    std::condition_variable cv_;
    
    KV_Store(const char* client_ip_address, const char* server_ip_address, size_t local_node_index) 
        : Node(client_ip_address, server_ip_address) {
        kv_map_ = new SOMap();
        get_queue_ = new SIMap();
        local_node_index_ = local_node_index;
    }

    KV_Store(size_t local_node_index) : Node() {
        kv_map_ = new SOMap();
        get_queue_ = new SIMap();
        local_node_index_ = local_node_index;
    }

    ~KV_Store() {
        delete kv_map_;
        delete get_queue_;
    }

    void put_map_(String* key_name, Serializer* value) {
        std::unique_lock<std::mutex> lock(kv_map_mutex_);
        kv_map_->put(key_name, value);
        lock.unlock();

        std::unique_lock<std::mutex> lck(get_queue_mutex_);
        int socket = get_queue_->remove(key_name);
        lck.unlock();
        if (socket > 0) {
            // TODO: lost target ip
            Value value_message(my_ip_, my_ip_, value);
            send_message(socket, &value_message);
        } else if (socket == 0) {
            cv_.notify_one();
        }
    }

    Serializer* get_map_(String* key_name) {
        return dynamic_cast<Serializer*>(kv_map_->get(key_name));
    }

    void put_get_queue_(String* key_name, int socket) {
        std::unique_lock<std::mutex> lock(get_queue_mutex_);
        get_queue_->put(key_name, socket);
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

    // Returns a new char array, make sure to delete it later
    char* get_value_serial(Key* key) {
        if (key->get_node_index() == local_node_index_) {
            Serializer* map_serial = get_map_(key->get_key());
            return map_serial->get_serial();
        }
        else {
            int index = other_node_indexes_->index_of(key->get_node_index());
            Get message(my_ip_, other_nodes_->get(index), key->get_key());
            Value* value_message = dynamic_cast<Value*>(send_message_to_node_wait(&message));
            char* serial = value_message->get_serial();
            delete value_message;
            return serial;
        }

    }

    char* wait_get_value_serial(Key* key) {
        if (key->get_node_index() == local_node_index_) {
            // TODO: actually wait
            Serializer* map_serial = get_map_(key->get_key());

            if (!map_serial) {
                // file descriptor 0 is stdin so this shouldn't be a socket descriptor
                get_queue_->put(key->get_key(), 0);
                std::unique_lock<std::mutex> lock(kv_map_mutex_);
                cv_.wait(lock);
                lock.unlock();
                map_serial = get_map_(key->get_key());
            }

            return map_serial->get_serial();
        }
        else {
            int index = other_node_indexes_->index_of(key->get_node_index());
            WaitGet message(my_ip_, other_nodes_->get(index), key->get_key());
            Value* value_message = dynamic_cast<Value*>(send_message_to_node_wait(&message));
            char* serial = value_message->get_serial();
            delete value_message;
            return serial;
        }

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

    // Returns a new FloatArray, make sure to delete it later
    FloatArray* get_float_array(Key* key) {
        char* kv_serial = get_value_serial(key);
        FloatArray* float_array = FloatArray::deserialize(kv_serial);
        delete[] kv_serial;
        return float_array;
    }

    // Returns a new StringArray, make sure to delete it later
    StringArray* get_string_array(Key* key) {
        char* kv_serial = get_value_serial(key);
        StringArray* string_array = StringArray::deserialize(kv_serial);
        delete[] kv_serial;
        return string_array;
    }

    // TODO: Cristian I think you wanted a different algorithm but I forget what is was
    size_t get_random_node_index() {
        int num_of_nodes = get_num_other_nodes();
        if (num_of_nodes == -1) {
            // no network set up
            return local_node_index_;
        }
        int index = rand() % num_of_nodes;

        return other_node_indexes_->get(index);
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
                    // TODO: think of a better way to do this than an assert, maybe send an Ack message with an error message attached
                }
                Value value_message(my_ip_, get_message->get_sender(), value);
                send_message(client_sockets_->get(client), &value_message);
                break;
            }
            case MsgKind::WaitGet: {
                WaitGet* get_message = dynamic_cast<WaitGet*>(message);
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
                assert(0);
        }
        return 1;
    }
};
