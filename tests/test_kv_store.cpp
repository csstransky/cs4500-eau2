#include <sys/wait.h>

#include "../src/kv_store/kv_store.h"
#include "../src/networks/rendezvous_server.h"

int LISTEN_TIME = 5;

void test_put_get() {
    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, &k);
    char* serial = kv.get_value_serial(&key);
    Deserializer deserializer(serial);
    String* kk = new String(deserializer);
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

void test_double_array() {
    DoubleArray* array = new DoubleArray(500);
    for (int i = 0; i < 500; i++) {
        array->push((double)i);
    }

    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, array);
    DoubleArray* result = kv.get_double_array(&key);

    for (int i = 0; i < 500; i++) {
        assert(result->get(i) == (double)i);
    }

    assert(array->equals(result));

    delete array;
    delete result;

    printf("KV Store double array test passed!\n");
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
    DoubleArray* double_array = new DoubleArray(500);
    for (int i = 0; i < 500; i++) {
        double_array->push((double)i);
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
        kv.put(&kf, double_array); 

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
        DoubleArray* double_result = kv.get_double_array(&kf); 
        assert(double_result->equals(double_array));
        delete double_result; 

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
    delete double_array;
    delete bool_array;
    delete string_array;

    printf("KV Store multiple objects test passed!\n");
}

void test_put_other_node() {
    int cpid[2];
    String* server_ip = new String("127.0.0.1");
    String* client_ip1 = new String("127.0.0.2");
    String* client_ip2 = new String("127.0.0.3");

    // Fork to create another process
    if ((cpid[0] = fork())) {
        
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        KV_Store* kv = new KV_Store(client_ip1->c_str(), server_ip->c_str(), 1);
        kv->connect_to_server(1);
        kv->run_server(-1);

        sleep(1);

        Key* key = new Key("key", 0);
        IntArray* array = new IntArray(1);
        array->push(1);
        Serializer* serial = new Serializer(array->serial_len());
        serial->serialize_object(array);
        kv->put(key, serial);

        kv->wait_for_shutdown();

        delete kv;
        delete key;
        delete array;
        delete serial;
        delete server_ip;
        delete client_ip1;
        delete client_ip2;

        // exit
        exit(0);
    }

    // Fork to create another process
    if ((cpid[1] = fork())) {
        
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        KV_Store* kv = new KV_Store(client_ip2->c_str(), server_ip->c_str(), 0);
        kv->connect_to_server(0);
        kv->run_server(-1);

        sleep(2);

        Key* key = new Key("key", 0);

        assert(kv->kv_map_->size() == 1);
        IntArray* array = kv->get_int_array(key);
        assert(array);
        assert(array->get(0) == 1);

        kv->wait_for_shutdown();

        delete kv;
        delete key;
        delete array;
        delete server_ip;
        delete client_ip1;
        delete client_ip2;

        // exit
        exit(0);
    }

    // In parent process

    // Start server
    RServer* server = new RServer(server_ip->c_str()); 
    server->run_server(LISTEN_TIME);
    server->wait_for_shutdown();

    // wait for child to finish
    int st;
    waitpid(cpid[0], &st, 0);
    waitpid(cpid[1], &st, 0);
    delete server;
    delete client_ip1;
    delete client_ip2;
    delete server_ip;

    printf("KV Store receive put message test passed!\n");
}

void test_get_other_node() {
    int cpid[2];
    String* server_ip = new String("127.0.0.1");
    String* client_ip1 = new String("127.0.0.2");
    String* client_ip2 = new String("127.0.0.3");

    // Fork to create another process
    if ((cpid[0] = fork())) {
        
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        KV_Store* kv = new KV_Store(client_ip1->c_str(), server_ip->c_str(), 1);
        kv->connect_to_server(1);
        kv->run_server(-1);

        sleep(1);

        Key* key = new Key("key", 1);
        IntArray* array = new IntArray(1);
        array->push(1);
        Serializer* serial = new Serializer(array->serial_len());
        serial->serialize_object(array);
        kv->put(key, serial);

        kv->wait_for_shutdown();

        delete kv;
        delete key;
        delete array;
        delete serial;
        delete server_ip;
        delete client_ip1;
        delete client_ip2;

        // exit
        exit(0);
    }

    // Fork to create another process
    if ((cpid[1] = fork())) {
        
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        KV_Store* kv = new KV_Store(client_ip2->c_str(), server_ip->c_str(), 0);
        kv->connect_to_server(0);
        kv->run_server(-1);

        sleep(2);

        Key* key = new Key("key", 1);

        IntArray* array = kv->get_int_array(key);
        assert(array);
        assert(array->get(0) == 1);

        kv->wait_for_shutdown();

        delete kv;
        delete key;
        delete array;
        delete server_ip;
        delete client_ip1;
        delete client_ip2;

        // exit
        exit(0);
    }

    // In parent process

    // Start server
    RServer* server = new RServer(server_ip->c_str()); 
    server->run_server(LISTEN_TIME);
    server->wait_for_shutdown();

    // wait for child to finish
    int st;
    waitpid(cpid[0], &st, 0);
    waitpid(cpid[1], &st, 0);
    delete server;
    delete client_ip1;
    delete client_ip2;
    delete server_ip;

    printf("KV Store get other node test passed!\n");
}

void test_wait_get() {
    int cpid[2];
    String* server_ip = new String("127.0.0.1");
    String* client_ip1 = new String("127.0.0.2");
    String* client_ip2 = new String("127.0.0.3");

    // Fork to create another process
    if ((cpid[0] = fork())) {
        
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        KV_Store* kv = new KV_Store(client_ip1->c_str(), server_ip->c_str(), 1);
        kv->connect_to_server(1);
        kv->run_server(-1);

        sleep(2);

        Key* key = new Key("key", 1);
        IntArray* array = new IntArray(1);
        array->push(1);
        Serializer* serial = new Serializer(array->serial_len());
        serial->serialize_object(array);

        assert(kv->get_queue_->size() == 1);
        assert(kv->get_queue_->get(key->get_key()) > 0);

        kv->put(key, serial);

        assert(kv->get_queue_->size() == 0);

        kv->wait_for_shutdown();

        delete kv;
        delete key;
        delete array;
        delete serial;
        delete server_ip;
        delete client_ip1;
        delete client_ip2;

        // exit
        exit(0);
    }

    // Fork to create another process
    if ((cpid[1] = fork())) {
        
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        KV_Store* kv = new KV_Store(client_ip2->c_str(), server_ip->c_str(), 0);
        kv->connect_to_server(0);
        kv->run_server(-1);

        sleep(1);

        Key* key = new Key("key", 1);

        char* serial = kv->wait_get_value_serial(key);
        IntArray* array = IntArray::deserialize(serial);
        assert(array);
        assert(array->get(0) == 1);

        kv->wait_for_shutdown();

        delete kv;
        delete key;
        delete array;
        delete[] serial;
        delete server_ip;
        delete client_ip1;
        delete client_ip2;

        // exit
        exit(0);
    }

    // In parent process

    // Start server
    RServer* server = new RServer(server_ip->c_str()); 
    server->run_server(LISTEN_TIME);
    server->wait_for_shutdown();

    // wait for child to finish
    int st;
    waitpid(cpid[0], &st, 0);
    waitpid(cpid[1], &st, 0);
    delete server;
    delete client_ip1;
    delete client_ip2;
    delete server_ip;

    printf("KV Store wait get test passed!\n");
}

void test_wait_local_get() {
    int cpid[2];
    String* server_ip = new String("127.0.0.1");
    String* client_ip1 = new String("127.0.0.2");
    String* client_ip2 = new String("127.0.0.3");

    // Fork to create another process
    if ((cpid[0] = fork())) {
        
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        KV_Store* kv = new KV_Store(client_ip1->c_str(), server_ip->c_str(), 1);
        kv->connect_to_server(1);
        kv->run_server(-1);

        sleep(2);

        Key* key = new Key("key", 0);
        IntArray* array = new IntArray(1);
        array->push(1);
        Serializer* serial = new Serializer(array->serial_len());
        serial->serialize_object(array);

        kv->put(key, serial);

        assert(kv->get_queue_->size() == 0);

        kv->wait_for_shutdown();

        delete kv;
        delete key;
        delete array;
        delete serial;
        delete server_ip;
        delete client_ip1;
        delete client_ip2;

        // exit
        exit(0);
    }

    // Fork to create another process
    if ((cpid[1] = fork())) {
        
    } else {
        // In child process

        // sleep .5s
        sleep(0.5);

        // start node
        KV_Store* kv = new KV_Store(client_ip2->c_str(), server_ip->c_str(), 0);
        kv->connect_to_server(0);
        kv->run_server(-1);

        sleep(1);

        Key* key = new Key("key", 0);

        char* serial = kv->wait_get_value_serial(key);
        IntArray* array = IntArray::deserialize(serial);
        assert(array);
        assert(array->get(0) == 1);

        kv->wait_for_shutdown();

        delete kv;
        delete key;
        delete array;
        delete[] serial;
        delete server_ip;
        delete client_ip1;
        delete client_ip2;

        // exit
        exit(0);
    }

    // In parent process

    // Start server
    RServer* server = new RServer(server_ip->c_str()); 
    server->run_server(LISTEN_TIME);
    server->wait_for_shutdown();

    // wait for child to finish
    int st;
    waitpid(cpid[0], &st, 0);
    waitpid(cpid[1], &st, 0);
    delete server;
    delete client_ip1;
    delete client_ip2;
    delete server_ip;

    printf("KV Store wait local get test passed!\n");
}

int main(int argc, char const *argv[]) {
    test_put_get();
    test_int_array();
    test_double_array();
    test_bool_array();
    test_string_array();
    test_multiple();
    test_put_other_node();
    test_get_other_node();
    test_wait_get();
    test_wait_local_get();
    printf("All KV Store test passed!\n");
}
