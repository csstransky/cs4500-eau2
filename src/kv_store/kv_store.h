#pragma once

#include "../helpers/object.h"
#include "../map/map.h"
#include "../dataframe/dataframe.h"
#include "key.h"

class KV_Store : public Object {
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
    void put(Key key, DataFrame* dataframe) {

    }

    void put(Key key, IntArray int_array) {

    }

    void put(Key key, BoolArray bool_array) {

    }

    void put(Key key, FloatArray float_array) {

    }

    void put(Key key, StringArray string_array) {

    } 

    DataFrame* get(Key key) {

    }
};
