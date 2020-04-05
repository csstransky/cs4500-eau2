#pragma once

#include "../dataframe/dataframe.h"
#include "../helpers/sor.h"

class KD_Store : public Object {
    public:
    KV_Store* kv_;

    KD_Store(size_t node_index) {
        kv_ = new KV_Store(node_index);
    }

    KD_Store(size_t node_index, const char* my_ip, const char* server_ip) {
        kv_ = new KV_Store(my_ip, server_ip, node_index);
        kv_->connect_to_server(node_index);
        kv_->run_server(200);
    }

    ~KD_Store() {
        delete kv_;
    }

    void application_complete() {
        kv_->wait_for_shutdown();
    }

    DataFrame* deserialize_df_(char* serial) {
        Deserializer deserializer(serial);
        DataFrame* df = new DataFrame(deserializer, kv_);
        delete[] serial;
        return df;
    }

    DataFrame* get(Key* key) { return deserialize_df_(kv_->get_value_serial(key)); }

    DataFrame* wait_and_get(Key* key) { return deserialize_df_(kv_->wait_get_value_serial(key)); }

    void put(Key* key, DataFrame* df) {
        kv_->put(key, df);
    }

    KV_Store* get_kv() {
        return kv_;
    }

};

// TODO: abstract this code later on with a helper function inside
// Moved here to remove circular dependency. See piazza post @963
DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, int* array) {
    Schema s("");
    DataFrame* d = new DataFrame(s, key->get_key(), kd->get_kv());
    Column col('I', kd->get_kv(), key->get_key(), 0);
    for (size_t i = 0; i < num; i++) {
        col.push_back(array[i]);
    }
    d->add_column(&col);
    
    kd->put(key, d);

    return d;
}

// Moved here to remove circular dependency. See piazza post @963
DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, double* array) {
    Schema s("");
    DataFrame* d = new DataFrame(s, key->get_key(), kd->get_kv());
    Column col('D', kd->get_kv(), key->get_key(), 0);
    for (size_t i = 0; i < num; i++) {
        col.push_back(array[i]);
    }
    d->add_column(&col);

    kd->put(key, d);

    return d;
}

// Moved here to remove circular dependency. See piazza post @963
DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, bool* array) {
    Schema s("");
    DataFrame* d = new DataFrame(s, key->get_key(), kd->get_kv());
    Column col('B', kd->get_kv(), key->get_key(), 0);
    for (size_t i = 0; i < num; i++) {
        col.push_back(array[i]);
    }
    d->add_column(&col);

    kd->put(key, d);

    return d;
}  

// Moved here to remove circular dependency. See piazza post @963
DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, String** array) {
    Schema s("");
    DataFrame* d = new DataFrame(s, key->get_key(), kd->get_kv());
    Column col('S', kd->get_kv(), key->get_key(), 0);
    for (size_t i = 0; i < num; i++) {
        col.push_back(array[i]);
    }
    d->add_column(&col);

    kd->put(key, d);

    return d;
}

DataFrame* DataFrame::from_scalar(Key* key, KD_Store* kd, int val) {
    int array[1];
    array[0] = val;
    return DataFrame::from_array(key, kd, 1, array);
}

DataFrame* DataFrame::from_scalar(Key* key, KD_Store* kd, double val) {
    double array[1];
    array[0] = val;
    return DataFrame::from_array(key, kd, 1, array);
}

DataFrame* DataFrame::from_scalar(Key* key, KD_Store* kd, bool val) {
    bool array[1];
    array[0] = val;
    return DataFrame::from_array(key, kd, 1, array);
}

DataFrame* DataFrame::from_scalar(Key* key, KD_Store* kd, String* val) {
    String* array[1];
    array[0] = val;
    return DataFrame::from_array(key, kd, 1, array);
}

DataFrame* DataFrame::from_file(Key* key, KD_Store* kd, char* file_name) {
    SoR sor(file_name, key->get_key(), kd->get_kv());
    kd->put(key, sor.get_dataframe());
    return sor.get_dataframe();
}

DataFrame* DataFrame::from_rower(Key* key, KD_Store* kd, const char* schema, Rower& rower) {
    Schema s(schema);
    DataFrame* df = new DataFrame(s, key->get_key(), kd->get_kv());
    Row r(s);
    while (!rower.accept(r)) df->add_row(r);
    df->add_row(r);
    kd->put(key, df);
    return df;
}
