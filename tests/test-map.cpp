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
    SOMap* map = new SOMap();
    t_true(map->isEmpty());
    t_true(map->size() == 0);
    String* a = new String("aa");
    String* b = new String("bb");
    String* aKey = new String("a");
    String* bKey = new String("b");
    t_false(map->containsKey(aKey));
    t_false(map->containsKey(bKey));
    t_false(map->containsValue(a));
    t_false(map->containsValue(b));
    map->put(aKey, a);
    map->put(bKey, b);
    t_true(map->containsKey(aKey));
    t_true(map->containsKey(bKey));
    t_true(map->containsValue(a));
    t_true(map->containsValue(b));
    t_true(map->size() == 2);
    t_false(map->isEmpty());
    t_true(a->equals(map->get(aKey)));
    t_true(b->equals(map->get(bKey)));
    Object* aold = map->remove(aKey);
    t_true(a->equals(aold));
    t_true(map->size() == 1);
    Object* bold = map->remove(bKey);
    t_true(b->equals(bold));
    t_true(map->size() == 0);
    t_true(map->isEmpty());
    map->put(aKey, a);
    map->put(bKey, b);
    t_true(map->size() == 2);
    String* newA = new String("newA");
    t_true(map->get(aKey)->equals(a));
    Object* oldA = map->put(aKey, newA);
    t_true(map->size() == 2);
    t_true(map->get(aKey)->equals(newA));
    SOMap* map2 = new SOMap();
    String* c = new String("cc");
    String* d = new String("dd");
    String* cKey = new String("c");
    String* dKey = new String("d");
    map2->put(cKey, c);
    map2->put(dKey, d);
    map->putAll(map2);
    t_true(map->size() == 4);
    // Since everyone has a different Array implementation, this is the most thorough that we can
    // test keySet and values. We just test to make sure that the function is returning an Array.
    Array* keys = nullptr;
    t_true(keys == nullptr);
    keys = map->keySet();
    t_true(keys != nullptr);
    Array* values = nullptr;
    t_true(values == nullptr);
    values = map->values();
    t_true(values != nullptr);
    map->clear();
    t_true(map->isEmpty());

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
    delete keys;
    delete values;
    delete oldA;
    delete aold;
    delete bold;
    
}

void testBasicSSMap () {
    SSMap* map = new SSMap();
    t_true(map->isEmpty());
    t_true(map->size() == 0);
    String* a = new String("aValue");
    String* b = new String("bValue");
    String* aKey = new String("a");
    String* bKey = new String("b");
    t_false(map->containsKey(aKey));
    t_false(map->containsKey(bKey));
    t_false(map->containsValue(a));
    t_false(map->containsValue(b));
    map->put(aKey, a);
    map->put(bKey, b);
    t_true(map->containsKey(aKey));
    t_true(map->containsKey(bKey));
    t_true(map->containsValue(a));
    t_true(map->containsValue(b));
    t_true(map->size() == 2);
    t_false(map->isEmpty());
    t_true(a->equals(map->get(aKey)));
    t_true(b->equals(map->get(bKey)));
    String* aold = map->remove(aKey);
    t_true(a->equals(aold));
    t_true(map->size() == 1);
    String* bold = map->remove(bKey);
    t_true(b->equals(bold));
    t_true(map->size() == 0);
    t_true(map->isEmpty());
    map->put(aKey, a);
    map->put(bKey, b);
    t_true(map->size() == 2);
    String* newA = new String("newAValue");
    t_true(map->get(aKey)->equals(a));
    String* oldA = map->put(aKey, newA);
    t_true(map->size() == 2);
    t_true(map->get(aKey)->equals(newA));
    SSMap* map2 = new SSMap();
    String* c = new String("cValue");
    String* d = new String("dValue");
    String* cKey = new String("c");
    String* dKey = new String("d");
    map2->put(cKey, c);
    map2->put(dKey, d);
    map->putAll(map2);
    t_true(map->size() == 4);
    // Since everyone has a different Array implementation, this is the most thorough that we can
    // test keySet and values. We just test to make sure that the function is returning an Array.
    Array* keys = nullptr;
    t_true(keys == nullptr);
    keys = map->keySet();
    t_true(keys != nullptr);
    Array* values = nullptr;
    t_true(values == nullptr);
    values = map->values();
    t_true(values != nullptr);
    map->clear();
    t_true(map->isEmpty());

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
    delete bold;
    delete aold;
    delete oldA;
    delete keys;
    delete values;
}

void testIncreaseMap() {
    SSMap* map = new SSMap();
    assert(map->isEmpty());
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
        String* result = map->get(&s);
        assert(result->equals(&value));
    }

    delete map;
} 

void testSIMap() {
    SIMap map;
    assert(map.isEmpty());
    assert(map.size() == 0);
    int size = 1000;

    char buf[20];
    for (int i = 0; i < size; i++) {
        snprintf(buf, 20, "k_%d", i);
        String s(buf);
        map.put(&s, i);
    }

    assert(map.size() == size);

    for (int i = 0; i < size; i++) {
        snprintf(buf, 20, "k_%d", i);
        String s(buf);
        int result = map.get(&s);
        assert(result == i);
    }

    for (int i = 0; i < size; i++) {
        snprintf(buf, 20, "k_%d", i);
        String s(buf);
        int result = map.remove(&s);
        assert(result == i);
    }

    assert(map.size() == 0);
}

int main() {
    testPair();
    testBasicSOMap();
    testBasicSSMap();
    testIncreaseMap();
    testSIMap();
}
