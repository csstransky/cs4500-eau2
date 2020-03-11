#include "../src/map/map.h"
#include <iostream>
#include <stdlib.h>
#include "../src/helpers/object.h"
#include "../src/helpers/string.h"
#include "../src/array/array.h"

void FAIL() { 
    printf("Fail\n");
    exit(1); 
}
void t_true(bool p) { if (!p) FAIL(); }
void t_false(bool p) { if (p) FAIL(); }

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

void testErrors() {
    
}

int main() {
    testBasicSOMap();
    testBasicSSMap();
}
