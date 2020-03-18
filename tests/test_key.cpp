// Made by Kaylin Devchand and Cristian Stransky

// #include <stdio.h> 
// #include <sys/socket.h> 
// #include <arpa/inet.h> 
// #include <unistd.h> 
// #include <string.h> 
// #include "../src/networks/message.h"
// #include "../src/array/array.h"
#include "../src/kv_store/key.h"
#include <cassert>

void constructor_test() {
    String key_string("main");
    size_t node_index = 0;
    Key* key1 = new Key(&key_string, node_index);
    Key* key2 = new Key(*key1);
    assert(key_string.equals(key1->get_key()));
    assert(key1->get_key()->equals(&key_string));
    assert(key1->get_node_index() == node_index);
    assert(key1->equals(key2));
    assert(key1->get_key()->equals(key2->get_key()));
    delete key1;

    assert(key_string.equals(key2->get_key()));
    assert(key2->get_key()->equals(&key_string));
    assert(key2->get_node_index() == node_index);
    delete key2;
    printf("Key constructor test passed!\n");
}

void clone_test() {
    String key_string("msain");
    size_t node_index = 2939;
    Key key1(&key_string, node_index);
    assert(key1.get_key()->equals(&key_string));
    assert(key1.get_node_index() == node_index);

    Key* key_clone = static_cast<Key*>(key1.clone());
    assert(key_clone->get_key()->equals(&key_string));
    assert(key_clone->get_node_index() == node_index);
    delete key_clone;
    printf("Key clone test passed!\n");
}

int main(int argc, char const *argv[]) 
{   
    constructor_test();
    clone_test();
    printf("All Key tests passed!\n");
    return 0;
} 