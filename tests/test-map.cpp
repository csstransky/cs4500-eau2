// Interface for Map and Tests made by @rohitppathak & @trevorstenson
// https://github.com/rohitppathak/-softdev-2020-ai-part2

#include <iostream>
#include <stdlib.h>
#include "../src/helpers/map.h"

void FAIL() { 
    printf("Fail\n");
    exit(1); 
}
void t_true(bool p) { if (!p) FAIL(); }
void t_false(bool p) { if (p) FAIL(); }

void testPair() {
    String* a = new String("a");
    String* b = new String("b");
    String* c = new String("c");
    Pair * pair1 = new Pair(a, b);

    t_true(pair1->get_key()->equals(a));
    t_true(pair1->get_value()->equals(b));

    Pair * pair2 = new Pair(a, b);
    t_true(pair1->equals(pair2));

    pair1->set_value(c);
    t_true(pair1->get_value()->equals(c));
    t_false(pair1->equals(pair2));
    t_false(pair1->hash() == pair2->hash());

    delete a;
    delete b;
    delete pair1;
    delete pair2;
    delete c;
}

void testBasicSOMap () {
    Map* map = new Map();
    t_true(map->size() == 0);
    String* a = new String("aa");
    String* b = new String("bb");
    String* aKey = new String("a");
    String* bKey = new String("b");
    map->put(aKey, a);
    map->put(bKey, b);
    t_true(map->size() == 2);
    t_true(a->equals(map->get(aKey)));
    t_true(b->equals(map->get(bKey)));
    Object* aold = map->remove(aKey);
    t_true(a->equals(aold));
    t_true(map->size() == 1);
    Object* bold = map->remove(bKey);
    t_true(b->equals(bold));
    t_true(map->size() == 0);
    map->put(aKey, a);
    map->put(bKey, b);
    t_true(map->size() == 2);
    String* newA = new String("newA");
    t_true(map->get(aKey)->equals(a));
    Object* oldA = map->put(aKey, newA);
    t_true(map->size() == 2);
    t_true(map->get(aKey)->equals(newA));
    Map* map2 = new Map();
    String* c = new String("cc");
    String* d = new String("dd");
    String* cKey = new String("c");
    String* dKey = new String("d");
    map2->put(cKey, c);
    map2->put(dKey, d);

    delete map;
    delete a;
    delete b;
    delete aKey;
    delete bKey;
    delete newA;
    delete map2;
    delete c;
    delete d;
    delete cKey;
    delete dKey;
    delete oldA;
    delete aold;
    delete bold;
    
}

void testIncreaseMap() {
    Map* map = new Map();
    assert(map->size() == 0);
    String value("value");
    int size = 1000;

    char buf[20];
    for (int i = 0; i < size; i++) {
        snprintf(buf, 20, "k_%d", i);
        String s(buf);
        map->put(&s, &value);
    }

    assert(map->size() == size);

    for (int i = 0; i < size; i++) {
        snprintf(buf, 20, "k_%d", i);
        String s(buf);
        String* result = dynamic_cast<String*>(map->get(&s));
        assert(result->equals(&value));
    }

    delete map;
} 

void testSIAMap() {
    Map map;
    assert(map.size() == 0);
    int size = 1000;

    char buf[20];
    
    for (int i = 0; i < size; i++) {
        snprintf(buf, 20, "k_%d", i);
        String s(buf);
        IntArray temp_array(size);
        temp_array.push(i);
        map.put(&s, &temp_array);
    }

    assert(map.size() == size);

    for (int i = 0; i < size; i++) {
        snprintf(buf, 20, "k_%d", i);
        String s(buf);
        IntArray* result = dynamic_cast<IntArray*>(map.get(&s));
        assert(result->get(0) == i);
    }

    for (int i = 0; i < size; i++) {
        snprintf(buf, 20, "k_%d", i);
        String s(buf);
        IntArray* result = dynamic_cast<IntArray*>(map.remove(&s));
        assert(result->get(0) == i);
        delete result;
    }

    assert(map.size() == 0);
}

void testSIMap() {
    SIMap map;
    assert(map.size() == 0);
    size_t size = 1000;

    char buf[20];
    
    for (size_t i = 0; i < size; i++) {
        snprintf(buf, 20, "k_%zu", i);
        String s(buf);
        Num n(i);
        map.put(&s, &n);
    }

    assert(map.size() == size);

    for (size_t i = 0; i < size; i++) {
        snprintf(buf, 20, "k_%zu", i);
        String s(buf);
        assert(map.get(&s)->v == i);
    }

    for (size_t i = 0; i < size; i++) {
        snprintf(buf, 20, "k_%zu", i);
        String s(buf);
        assert(map.remove(&s)->v == i);
    }

    assert(map.size() == 0);
}

int main() {
    testPair();
    testBasicSOMap();
    testIncreaseMap();
    testSIAMap();
    testSIMap();
}
