#pragma once

#include "../helpers/object.h"
#include "../map/map.h"
#include "../dataframe/dataframe.h"
#include "key.h"

class KV_Store : public Object {
    public:
    SOMap* kv_map_; // Key.key -> Serializer
    size_t local_node_index_;

    KV_Store(size_t local_node_index) {
        kv_map_ = new SOMap();
        local_node_index_ = local_node_index;
    }

    ~KV_Store() {
        delete kv_map_;
    }

    // TODO: The put will happen inside DataFrame::from_array(&key, &kv, SZ, vals);
    // void put(Key* key, DataFrame* dataframe) {

    // }

    void put(Key* key, Object* value) {
        if (key->get_node_index() == local_node_index_) {
            Serializer serial(value->serial_len());
            serial.serialize_object(value);
            kv_map_->put(key->get_key(), static_cast<Object*>(&serial));
        } 
        else {
            // TODO call upon another Node to put the kv
            assert(0);
        }
    }

    // Returns a new char array, make sure to delete it later
    char* get_value_serial(Key* key) {
        String* key_string = key->get_key();
        Serializer* map_serial = static_cast<Serializer*>(kv_map_->get(key_string));
        return map_serial->get_serial();
    }

    // // TODO: also need to get arrays. Either have multiple gets or return Object*
    // DataFrame* get(Key* key) {
        // TODO
    // }

    Object* get(Key* key) {

    }

    // Returns a new IntArray, make sure to delete it later
    IntArray* get_int_array(Key* key) {
        char* kv_serial = get_value_serial(key);
        IntArray* int_array = IntArray::deserialize(kv_serial);
        delete kv_serial;
        return int_array;
    }

    // Returns a new BoolArray, make sure to delete it later
    BoolArray* get_bool_array(Key* key) {
        char* kv_serial = get_value_serial(key);
        BoolArray* bool_array = BoolArray::deserialize(kv_serial);
        delete kv_serial;
        return bool_array;
    }

    // Returns a new FloatArray, make sure to delete it later
    FloatArray* get_float_array(Key* key) {
        char* kv_serial = get_value_serial(key);
        FloatArray* float_array = FloatArray::deserialize(kv_serial);
        delete kv_serial;
        return float_array;
    }

    // Returns a new StringArray, make sure to delete it later
    StringArray* get_string_array(Key* key) {
        char* kv_serial = get_value_serial(key);
        StringArray* string_array = StringArray::deserialize(kv_serial);
        delete kv_serial;
        return string_array;
    }
};
