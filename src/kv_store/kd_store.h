#pragma once

#include "dataframe.h"

class KD_Store : public Object {
    public:
    KV_Store* kv_;

    KD_Store(KV_Store* kv) {
        kv_ = kv;
    }

    DataFrame* get(Key* key) {
        char* kv_serial = kv_->get_value_serial(key);
        DataFrame* df = DataFrame::deserialize(kv_serial);
        delete kv_serial;
        return df; 
    }

    void put(Key* key, DataFrame* df) {
        kv_->put(key, df);
    }

    KV_Store* get_kv() {
        return kv_;
    }

};

// Moved here to remove circular dependency. See piazza post @963
DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, int* array) {
    Schema s("I");
    DataFrame* d = new DataFrame(s, key->get_key(), kd->get_kv());
    for (size_t i = 0; i < num; i++) {
        IntColumn* col = d->get_column(0)->as_int();
        col->push_back(array[i]);
    }

    return d;
}

// Moved here to remove circular dependency. See piazza post @963
DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, float* array) {
    Schema s("F");
    DataFrame* d = new DataFrame(s, key->get_key(), kd->get_kv());
    for (size_t i = 0; i < num; i++) {
        FloatColumn* col = d->get_column(0)->as_float();
        col->push_back(array[i]);
    }

    return d;
}

// Moved here to remove circular dependency. See piazza post @963
DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, bool* array) {
    Schema s("B");
    DataFrame* d = new DataFrame(s, key->get_key(), kd->get_kv());
    for (size_t i = 0; i < num; i++) {
        BoolColumn* col = d->get_column(0)->as_bool();
        col->push_back(array[i]);
    }

    return d;
}  

// Moved here to remove circular dependency. See piazza post @963
DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, String** array) {
    Schema s("S");
    DataFrame* d = new DataFrame(s, key->get_key(), kd->get_kv());
    for (size_t i = 0; i < num; i++) {
        StringColumn* col = d->get_column(0)->as_string();
        col->push_back(array[i]);
    }

    return d;
 }