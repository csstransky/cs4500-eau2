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

    KV_Store(size_t local_node_index) : Node(nullptr, nullptr) {
        kv_map_ = new SOMap();
        local_node_index_ = local_node_index;
    }

    ~KV_Store() {
        delete kv_map_;
    }

    void put_map_(String* key_name, Serializer* value) {
        kv_map_->put(key_name, value);
    }

    void put(Key* key, Object* value) {
        Serializer serial(value->serial_len());
        serial.serialize_object(value);
        if (key->get_node_index() == local_node_index_) {
            put_map_(key->get_key(), &serial);
        } 
        else {
            // TODO: call upon another Node to put the kv
            assert(0);
        }
    }

    size_t get_node_index() {
        return local_node_index_;
    }

    // Returns a new char array, make sure to delete it later
    char* get_value_serial(Key* key) {
        if (key->get_node_index() == local_node_index_) {
            String* key_string = key->get_key();
            Serializer* map_serial = static_cast<Serializer*>(kv_map_->get(key_string));
            return map_serial->get_serial();
        }
        else {
            // TODO: call upon another Node to get the kv
            assert(0);
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
};
