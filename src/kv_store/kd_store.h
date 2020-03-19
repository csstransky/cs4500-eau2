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

};