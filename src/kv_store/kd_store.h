#pragma once

#include "../dataframe/dataframe.h"
#include "../helpers/sor.h"
#include "../dataframe/dataframe_builder.h"

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

// TODO: To keep Jan's convention of using specific type of arrays in his assignment code, we have
// to make this funky helper function, but maybe we can go back to those Application files and make
// them use OUR arrays
DataFrame* make_new_dataframe_from_array_(Key* key, KD_Store* kd, size_t num, const char* type, 
    int* int_array, double* double_array, bool* bool_array, String** string_array) {
    DataFrameBuilder df_builder(type, key->get_key(), kd->get_kv());
    Schema s(type);
    Row r(s);
    for (size_t ii = 0; ii < num; ii++) {
        switch(type[0]) {
            case 'I': r.set(0, int_array[ii]); break;
            case 'D': r.set(0, double_array[ii]); break;
            case 'B': r.set(0, bool_array[ii]); break;
            case 'S': r.set(0, string_array[ii]); break;
        }
        df_builder.add_row(r);
    }
    DataFrame* d = df_builder.done();
    kd->put(key, d);
    return d;
}

DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, int* array) {
    return make_new_dataframe_from_array_(key, kd, num, "I", array, nullptr, nullptr, nullptr);
}
DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, double* array) {
    return make_new_dataframe_from_array_(key, kd, num, "D", nullptr, array, nullptr, nullptr);
}
DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, bool* array) {
    return make_new_dataframe_from_array_(key, kd, num, "B", nullptr, nullptr, array, nullptr);
}  
DataFrame* DataFrame::from_array(Key* key, KD_Store* kd, size_t num, String** array) {
    return make_new_dataframe_from_array_(key, kd, num, "S", nullptr, nullptr, nullptr, array);
}
DataFrame* DataFrame::from_scalar(Key* key, KD_Store* kd, int val) { return DataFrame::from_array(key, kd, 1, &val); }
DataFrame* DataFrame::from_scalar(Key* key, KD_Store* kd, double val) { return DataFrame::from_array(key, kd, 1, &val); }
DataFrame* DataFrame::from_scalar(Key* key, KD_Store* kd, bool val) { return DataFrame::from_array(key, kd, 1, &val); }
DataFrame* DataFrame::from_scalar(Key* key, KD_Store* kd, String* val) { return DataFrame::from_array(key, kd, 1, &val); }

DataFrame* DataFrame::from_file(Key* key, KD_Store* kd, char* file_name) {
    SoR sor(file_name, key->get_key(), kd->get_kv());
    kd->put(key, sor.get_dataframe());
    return sor.get_dataframe();
}

DataFrame* DataFrame::from_rower(Key* key, KD_Store* kd, const char* schema, Rower& rower) {
    Schema s(schema);
    DataFrameBuilder df_builder(schema, key->get_key(), kd->get_kv());
    Row r(s);
    while (!rower.accept(r)) df_builder.add_row(r);
    DataFrame* df = df_builder.done();
    kd->put(key, df);
    return df;
}
