#include "../src/kv_store/kv_store.h"

void test_put_get() {
    String k("k");
    Key key(&k, 0);
    KV_Store kv(0);

    kv.put(&key, &k);
    char* serial = kv.get_value_serial(&key);
    String* kk = String::deserialize(serial);
    assert(k.equals(kk));

    printf("KV Store put get test passed!\n");
}

int main(int argc, char const *argv[]) {
    test_put_get();
    printf("All KV Store test passed!\n");
}
