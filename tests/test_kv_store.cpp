#include "../src/kv_store/kv_store.h"

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
    IntArray* array = new IntArray(100);
    for (int i = 0; i < 100; i++) {
        array->push(i);
    }

    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, array);
    IntArray* result = kv.get_int_array(&key);

    for (int i = 0; i < 100; i++) {
        assert(result->get(i) == i);
    }

    assert(array->equals(result));

    delete array;
    delete result;

    printf("KV Store int array test passed!\n");
}

void test_float_array() {
    FloatArray* array = new FloatArray(100);
    for (int i = 0; i < 100; i++) {
        array->push((float)i);
    }

    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, array);
    FloatArray* result = kv.get_float_array(&key);

    for (int i = 0; i < 100; i++) {
        assert(result->get(i) == (float)i);
    }

    assert(array->equals(result));

    delete array;
    delete result;

    printf("KV Store float array test passed!\n");
}

void test_bool_array() {
    BoolArray* array = new BoolArray(100);
    for (int i = 0; i < 100; i++) {
        array->push((bool)i % 2);
    }

    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, array);
    BoolArray* result = kv.get_bool_array(&key);

    for (int i = 0; i < 100; i++) {
        assert(result->get(i) == (bool)i%2);
    }

    assert(array->equals(result));

    delete array;
    delete result;

    printf("KV Store bool array test passed!\n");
}

void test_string_array() {
    String s("s");
    StringArray* array = new StringArray(100);
    for (int i = 0; i < 100; i++) {
        array->push(&s);
    }

    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, array);
    StringArray* result = kv.get_string_array(&key);

    for (int i = 0; i < 100; i++) {
        assert(result->get(i)->equals(&s));
    }

    assert(array->equals(result));

    delete array;
    delete result;

    printf("KV Store string array test passed!\n");
}

void test_multiple() {
    IntArray* int_array = new IntArray(100);
    for (int i = 0; i < 100; i++) {
        int_array->push(i);
    } 
    FloatArray* float_array = new FloatArray(100);
    for (int i = 0; i < 100; i++) {
        float_array->push((float)i);
    } 
    BoolArray* bool_array = new BoolArray(100);
    for (int i = 0; i < 100; i++) {
        bool_array->push((bool)i % 2);
    } 
    String s("s");
    StringArray* string_array = new StringArray(100);
    for (int i = 0; i < 100; i++) {
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

int main(int argc, char const *argv[]) {
    test_put_get();
    test_int_array();
    test_float_array();
    test_bool_array();
    test_string_array();
    test_multiple();
    printf("All KV Store test passed!\n");
}
