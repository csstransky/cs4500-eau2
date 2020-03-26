#include <sys/wait.h>

#include "../src/kv_store/kv_store.h"
#include "../src/networks/rendezvous_server.h"

void test_put_get() {
    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, &k);
    char* serial = kv.get_value_serial(&key);
    String* kk = String::deserialize(serial);
    delete[] serial;
    assert(k.equals(kk));
    delete kk;

    printf("KV Store put get test passed!\n");
}

void test_int_array() {
    IntArray* array = new IntArray(500);
    for (int i = 0; i < 500; i++) {
        array->push(i);
    }

    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, array);
    IntArray* result = kv.get_int_array(&key);

    for (int i = 0; i < 500; i++) {
        assert(result->get(i) == i);
    }

    assert(array->equals(result));

    delete array;
    delete result;

    printf("KV Store int array test passed!\n");
}

void test_float_array() {
    FloatArray* array = new FloatArray(500);
    for (int i = 0; i < 500; i++) {
        array->push((float)i);
    }

    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, array);
    FloatArray* result = kv.get_float_array(&key);

    for (int i = 0; i < 500; i++) {
        assert(result->get(i) == (float)i);
    }

    assert(array->equals(result));

    delete array;
    delete result;

    printf("KV Store float array test passed!\n");
}

void test_bool_array() {
    BoolArray* array = new BoolArray(500);
    for (int i = 0; i < 500; i++) {
        array->push((bool)i % 2);
    }

    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, array);
    BoolArray* result = kv.get_bool_array(&key);

    for (int i = 0; i < 500; i++) {
        assert(result->get(i) == (bool)i%2);
    }

    assert(array->equals(result));

    delete array;
    delete result;

    printf("KV Store bool array test passed!\n");
}

void test_string_array() {
    String s("s");
    StringArray* array = new StringArray(500);
    for (int i = 0; i < 500; i++) {
        array->push(&s);
    }

    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, array);
    StringArray* result = kv.get_string_array(&key);

    for (int i = 0; i < 500; i++) {
        assert(result->get(i)->equals(&s));
    }

    assert(array->equals(result));

    delete array;
    delete result;

    printf("KV Store string array test passed!\n");
}

void test_multiple() {
    IntArray* int_array = new IntArray(500);
    for (int i = 0; i < 500; i++) {
        int_array->push(i);
    } 
    FloatArray* float_array = new FloatArray(500);
    for (int i = 0; i < 500; i++) {
        float_array->push((float)i);
    } 
    BoolArray* bool_array = new BoolArray(500);
    for (int i = 0; i < 500; i++) {
        bool_array->push((bool)i % 2);
    } 
    String s("s");
    StringArray* string_array = new StringArray(500);
    for (int i = 0; i < 500; i++) {
        string_array->push(&s);
    }

    char buf[6];
    KV_Store kv(0);

    for (int i = 0; i < 5; i++) {
        snprintf(buf, 6, "ki_%d", i);
        String si(buf);
        Key ki(&si, 0);
        kv.put(&ki, int_array); 

        snprintf(buf, 6, "kf_%d", i);
        String sf(buf);
        Key kf(&sf, 0);
        kv.put(&kf, float_array); 

        snprintf(buf, 6, "kb_%d", i);
        String sb(buf);
        Key kb(&sb, 0);
        kv.put(&kb, bool_array); 

        snprintf(buf, 6, "ks_%d", i);
        String ss(buf);
        Key ks(&ss, 0);
        kv.put(&ks, string_array); 
    }

    for (int i = 0; i < 5; i++) {
        snprintf(buf, 6, "ki_%d", i);
        String si(buf);
        Key ki(&si, 0);
        IntArray* int_result = kv.get_int_array(&ki); 
        assert(int_result->equals(int_array));
        delete int_result;

        snprintf(buf, 6, "kf_%d", i);
        String sf(buf);
        Key kf(&sf, 0);
        FloatArray* float_result = kv.get_float_array(&kf); 
        assert(float_result->equals(float_array));
        delete float_result; 

        snprintf(buf, 6, "kb_%d", i);
        String sb(buf);
        Key kb(&sb, 0);
        BoolArray* bool_result = kv.get_bool_array(&kb); 
        assert(bool_result->equals(bool_array));
        delete bool_result;

        snprintf(buf, 6, "ks_%d", i);
        String ss(buf);
        Key ks(&ss, 0);
        StringArray* string_result = kv.get_string_array(&ks); 
        assert(string_result->equals(string_array));
        delete string_result;
    }

    delete int_array;
    delete float_array;
    delete bool_array;
    delete string_array;

    printf("KV Store multiple objects test passed!\n");
}

void test_receive_put() {
    int cpid;
    String* server_ip = new String("127.0.0.1");
    String* client_ip = new String("127.0.0.2");

    // Fork to create another process
    if ((cpid = fork())) {
        // In parent process

        // Start server
        RServer* server = new RServer(server_ip->c_str()); 
        server->run_server(2);

        // check for registered client
        assert(server->client_sockets_->length() == 1);
        assert(server->connected_client_ips_->length() == 1);
        assert(server->connected_client_ips_->get(0)->equals(client_ip));
        assert(server->node_indexes_->length() == 1);
        assert(server->node_indexes_->get(0) == 0);

        Key key("key", 0);
        IntArray array(1);
        array.push(1);
        Serializer serial(array.serial_len());
        serial.serialize_object(&array);
        Put message(server_ip, client_ip, key.get_key(), &serial);
        server->send_message(client_ip, &message);
        
        // wait for child to finish
        int st;
        waitpid(cpid, &st, 0);
        server->run_server(0.5);
        server->shutdown();
        delete server;
        delete client_ip;
        delete server_ip;
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        KV_Store* kv = new KV_Store(client_ip->c_str(), server_ip->c_str(), 0);
        kv->connect_to_server(0);
        kv->run_server(5);

        Key* key = new Key("key", 0);

        assert(kv->kv_map_->size() == 1);
        IntArray* array = kv->get_int_array(key);
        assert(array);
        assert(array->get(0) == 1);

        delete kv;
        delete key;
        delete array;
        delete server_ip;
        delete client_ip;

        // exit
        exit(0);
    }

    printf("KV Store receive put message test passed!\n");
}

int main(int argc, char const *argv[]) {
    test_put_get();
    test_int_array();
    test_float_array();
    test_bool_array();
    test_string_array();
    test_multiple();
    test_receive_put();
    printf("All KV Store test passed!\n");
}
