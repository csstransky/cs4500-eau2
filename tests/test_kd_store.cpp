#include <sys/wait.h>

#define TEST

#include "../src/kv_store/kd_store.h"
#include "../src/networks/rendezvous_server.h"

int LISTEN_TIME = 5;

void test_one_dataframe() {
    Key key("key", 0);
    KD_Store kd(0);

    int* array = new int[200];
    for (int i = 0; i < 200; i++) {
        array[i] = i;
    }

    DataFrame* df = DataFrame::from_array(&key, &kd, 200, array);
    DataFrame* df2 = kd.get(&key);

    assert(df2);
    assert(df2->nrows() == 200);
    assert(df2->ncols() == 1);

    for (int i = 0; i < 200; i++) {
        assert(df2->get_int(0, i) == i);
    }

    delete df;
    delete[] array;
    delete df2;

    printf("KD Store one dataframe tests pass!\n");
}

void test_multiple_dataframe() {
    size_t rows = 200;
    Key key("key", 0);
    Key key2("key2", 0);
    KD_Store kd(0);

    DataFrameBuilder df_builder("IDBS", key.get_key(), kd.get_kv());
    DataFrameBuilder df_builder2("I", key2.get_key(), kd.get_kv());

    String test("test");
    Schema s("IDBS");
    Row r(s);
    for (size_t i = 0; i < rows; i++) {
        r.set(0, (int)i);
        r.set(1, (double)i + 0.1);
        r.set(2, (bool)(i % 2));
        r.set(3, &test);
        df_builder.add_row(r);
        df_builder2.add_row(r);
    }

    DataFrame* df = df_builder.done();
    DataFrame* df3 = df_builder2.done();

    assert(df->get_schema().col_type(0) == 'I');
    assert(df->get_schema().col_type(1) == 'D');
    assert(df->get_schema().col_type(2) == 'B');
    assert(df->get_schema().col_type(3) == 'S');

    for (size_t i = 0; i < rows; i++) {
        assert(df3->get_int(0, i) == i);
        assert(df->get_int(0, i) == i);
        assert(df->get_double(1, i) == (double)(i+0.1));
        assert(df->get_bool(2, i) == i % 2);
        assert(df->get_string(3, i)->equals(&test));
    }

    kd.put(&key, df);
    kd.put(&key2, df3);
    DataFrame* df2 = kd.get(&key);
    DataFrame* df4 = kd.get(&key2);

    assert(df2);
    assert(df2->nrows() == rows);
    assert(df2->ncols() == 4);

    assert(df4);
    assert(df4->nrows() == rows);
    assert(df4->ncols() == 1);

    for (size_t i = 0; i < rows; i++) {
        assert(df4->get_int(0, i) == i);
        assert(df2->get_int(0, i) == i);
        assert(df2->get_double(1, i) == (double)(i+0.1));
        assert(df2->get_bool(2, i) == i % 2);
        assert(df2->get_string(3, i)->equals(&test));
    }

    delete df;
    delete df3;
    delete df2;
    delete df4;

    printf("KD Store multiple dataframe tests pass!\n");
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
        KD_Store* kd = new KD_Store(1, client_ip1->c_str(), server_ip->c_str());

        sleep(1);

        Key* key = new Key("key", 0);
        int* array = new int[10];
        for (int i = 0; i < 10; i++) {
            array[i] = i+1;
        }
        DataFrame* df = DataFrame::from_array(key, kd, 10, array);

        kd->application_complete();

        delete kd;
        delete key;
        delete[] array;
        delete df;
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
        KD_Store* kd = new KD_Store(0, client_ip2->c_str(), server_ip->c_str());

        sleep(3);

        Key* key = new Key("key", 0);

        assert(kd->kv_->kv_map_->size() > 1);
        DataFrame* df = kd->get(key);
        assert(df);
        for (int i = 0; i < 10; i++) {
            assert(df->get_int(0, i) == i+1);
        }

        kd->application_complete();

        delete kd;
        delete key;
        delete df;
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

    printf("KD Store put test passed!\n");
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
        KD_Store* kd = new KD_Store(1, client_ip1->c_str(), server_ip->c_str());

        sleep(1);

        Key* key = new Key("key", 1);
        int* array = new int[10];
        for (int i = 0; i < 10; i++) {
            array[i] = i+1;
        }
        DataFrame* df = DataFrame::from_array(key, kd, 10, array);

        kd->application_complete();

        delete kd;
        delete key;
        delete[] array;
        delete df;
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
        KD_Store* kd = new KD_Store(0, client_ip2->c_str(), server_ip->c_str());

        sleep(2);

        Key* key = new Key("key", 1);

        DataFrame* df = kd->get(key);
        assert(df);
        for (int i = 0; i < 10; i++) {
            assert(df->get_int(0, i) == i+1);
        }

        kd->application_complete();

        delete kd;
        delete key;
        delete df;
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

    printf("KD Store get other node test passed!\n");
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
        KD_Store* kd = new KD_Store(1, client_ip1->c_str(), server_ip->c_str());

        sleep(2);

        Key* key = new Key("key", 1);
        int* array = new int[10];
        for (int i = 0; i < 10; i++) {
            array[i] = i+1;
        }

        assert(kd->kv_->get_queue_->size() == 1);
        assert(kd->kv_->get_queue_->get(key->get_key()) != nullptr);

        DataFrame* df = DataFrame::from_array(key, kd, 10, array);

        assert(kd->kv_->get_queue_->size() == 0);

        kd->application_complete();


        delete kd;
        delete key;
        delete[] array;
        delete df;
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
        KD_Store* kd = new KD_Store(0, client_ip2->c_str(), server_ip->c_str());

        sleep(1);

        Key* key = new Key("key", 1);

        DataFrame* df = kd->wait_and_get(key);
        assert(df);
        for (int i = 0; i < 10; i++) {
            assert(df->get_int(0, i) == i+1);
        }

        kd->application_complete();

        delete kd;
        delete key;
        delete df;
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

    printf("KD Store wait get test passed!\n");
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
        KD_Store* kd = new KD_Store(1, client_ip1->c_str(), server_ip->c_str());

        sleep(2);

        Key* key = new Key("key", 0);
        int* array = new int[10];
        for (int i = 0; i < 10; i++) {
            array[i] = i+1;
        }

        DataFrame* df = DataFrame::from_array(key, kd, 10, array);

        assert(kd->kv_->get_queue_->size() == 0);

        kd->application_complete();


        delete kd;
        delete key;
        delete[] array;
        delete df;
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
        KD_Store* kd = new KD_Store(0, client_ip2->c_str(), server_ip->c_str());

        sleep(1);

        Key* key = new Key("key", 0);

        DataFrame* df = kd->wait_and_get(key);
        assert(df);
        for (int i = 0; i < 10; i++) {
            assert(df->get_int(0, i) == i+1);
        }

        kd->application_complete();

        delete kd;
        delete key;
        delete df;
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

    printf("KD Store wait local get test passed!\n");
}

int main(int argc, char** argv) {
    test_one_dataframe();
    test_multiple_dataframe();
    test_put_other_node();
    test_get_other_node();
    test_wait_get();
    test_wait_local_get();
    printf("All KD Store tests pass!\n");
}