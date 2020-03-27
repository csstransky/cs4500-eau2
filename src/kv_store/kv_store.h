#pragma once

#include "../helpers/map.h"
#include "key.h"
#include "../networks/node.h"

class KV_Store : public Node {
    public:
    SOMap* kv_map_; // String* -> Serializer* 
    size_t local_node_index_;
    
    KV_Store(const char* client_ip_address, const char* server_ip_address, size_t local_node_index) 
        : Node(client_ip_address, server_ip_address) {
        kv_map_ = new SOMap();
        local_node_index_ = local_node_index;
    }

    KV_Store(size_t local_node_index) : Node() {
        kv_map_ = new SOMap();
        local_node_index_ = local_node_index;
    }

    ~KV_Store() {
        delete kv_map_;
    }

    void put_map_(String* key_name, Serializer* value) {
        kv_map_->put(key_name, value);
    }

    Serializer* get_map_(String* key_name) {
        return dynamic_cast<Serializer*>(kv_map_->get(key_name));
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
                Value value_message(my_ip_, get_message->get_sender(), value);
                send_message(client_sockets_->get(client), &value_message);
                break;
            }   
            default:
                // Nobody inherits from kv store so it has to handle the message
                assert(0);
        }
        return 1;
    }
};
