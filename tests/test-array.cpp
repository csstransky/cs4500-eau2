// Interface for Array and Tests made by @chasebish & @oriasm
// https://github.com/chasebish/cs4500_assignment1_part2

#include <stdlib.h>
#include <stdio.h>
#include "../src/helpers/array.h"
#include "../src/dataframe/column_array.h"
#include "../src/kv_store/key_array.h"

void FAIL(const char* m) {
  fprintf(stderr, "test %s failed\n", m);
  exit(1);
}
void OK(const char* m) { printf("test %s passed\n", m); }
void t_true(bool p, const char* m) { if (!p) FAIL(m); }
void t_false(bool p, const char* m) { if (p) FAIL(m); }

/** Tests string equality */
void basic_object_test() {
  Object * x = new Object();
  Object * y = new Object();
  Object * z = new Object();

  t_true(x->equals(x), "1a");
  t_true(z->equals(z), "1b");
  t_false(x->equals(y), "1c");
  t_false(x->equals(y), "1d");

  delete z;
  delete y;
  delete x;

  OK("1");
}


/** Tests String equality */
void basic_string_test() {
  String * x = new String("x");
  String * x_copy = new String("x");
  String * y = new String("y");
  String * z = new String("z");

  t_true(x->equals(x_copy), "2a");
  t_false(x->equals(y), "2b");
  t_false(x->equals(z), "2b");

  delete z;
  delete y;
  delete x_copy;
  delete x;

  OK("2");
}

/** Tests pushing, popping, and length of Arrays */
void basic_stringarray_test() {
  String * x = new String("Hello");
  String * y = new String("World");
  String * z = new String("!");
  StringArray * arr = new StringArray(10);

  arr->push(x);
  arr->push(y);
  arr->push(z);
  t_true(arr->length() == 3, "3a");
  arr->clear();
  t_true(arr->length() == 0, "3c");

  delete arr;
  delete z;
  delete y;
  delete x;

  OK("3");
}

/** Tests pushing, popping, and length of Arrays */
void basic_intarray_test() {
  IntArray * arr = new IntArray(10);

  arr->push(1);
  arr->push(2);
  arr->push(3);
  t_true(arr->length() == 3, "4a");
  arr->clear();
  t_true(arr->length() == 0, "4c");

  delete arr;

  OK("4");
}

/** Tests pushing, popping, and length of Arrays */
void basic_DoubleArray_tests() {
  DoubleArray * arr = new DoubleArray(10);

  arr->push(1.5);
  arr->push(2.7);
  arr->push(3.9);
  t_true(arr->length() == 3, "5a");
  arr->clear();
  t_true(arr->length() == 0, "5c");

  delete arr;

  OK("5");
}

/** Tests pushing, popping, and length of Arrays */
void basic_boolarray_tests() {
  BoolArray * arr = new BoolArray(10);

  arr->push(false);
  arr->push(true);
  arr->push(false);
  t_true(arr->length() == 3, "6a");
  arr->clear();
  t_true(arr->length() == 0, "6c");

  delete arr;

  OK("3");
}

/** Tests more complex Array functions */
void complex_stringarray_test() {
  String a("This");
  String b("is going");
  String c("in a list");
  String x("Hello");
  String y("World");
  String z("!");
  StringArray arr1(10);
  StringArray arr2(10);

  arr1.push(&a);
  arr1.push(&b);
  arr1.push(&c);
  arr2.push(&x);
  arr2.push(&y);
  arr2.push(&z);
  String * pointer_of_a = arr1.get(0);
  t_true(pointer_of_a->equals(&a), "7a");
  String* v = arr2.remove(2);
  t_true(v->equals(&z), "7b");
  delete v;
  t_true(arr2.index_of(&z) == -1, "7g");
  String* w = arr2.replace(1, &z);
  t_true(y.equals(w), "7h");
  delete w;
  t_true(arr2.index_of(&y) == -1, "7i");
  StringArray * copy_of_arr1 = new StringArray(arr1);
  t_true(copy_of_arr1->equals(&arr1), "7j");
  delete copy_of_arr1;

  OK("7");
}

/** Tests object array */
void object_array_test() {
  String * a = new String("a");
  String * b = new String("b");
  String * c = new String("c");
  ObjectArray * arr1 = new ObjectArray(10);

  arr1->push(a);
  t_true(arr1->length() == 1, "8a");
  t_true(arr1->get(0)->equals(a), "8b");

  arr1->push(b);
  t_true(arr1->length() == 2, "8c");
  t_true(arr1->get(1)->equals(b), "8d");

  Object * d = arr1->replace(0, c);
  t_true(d->equals(a), "8e");
  t_true(arr1->get(0)->equals(c), "8f");
  t_true(arr1->length() == 2, "8g");

  Object * e = arr1->remove(0);
  t_true(e->equals(c), "8h");
  t_true(arr1->get(0)->equals(b), "8i");
  t_true(arr1->length() == 1, "8j");

  delete a;
  delete b;
  delete c;
  delete d;
  delete e;
  delete arr1;

  OK("8");
}

void clone_intarray_test() {
  IntArray arr(10);
  arr.push(1);
  arr.push(2);
  arr.push(3);
  t_true(arr.length() == 3, "9a");
  IntArray* clone = arr.clone();
  t_true(clone->length() == 3, "9b");
  t_true(clone->get(0) == 1, "9c");
  t_true(clone->get(1) == 2, "9d");
  t_true(clone->get(2) == 3, "9e");

  delete clone;
  OK("9");
}

void clone_DoubleArray_test() {
  DoubleArray arr(10);

  arr.push(1.5);
  arr.push(2.7);
  arr.push(3.9);
  t_true(arr.length() == 3, "10a");
  DoubleArray* clone = arr.clone();
  t_true(clone->length() == 3, "10b");
  t_true(clone->get(0) == (double)1.5, "10c");
  t_true(clone->get(1) == (double)2.7, "10d");
  t_true(clone->get(2) == (double)3.9, "10e");

  delete clone;
  OK("10");
}

void clone_boolarray_test() {
  BoolArray arr(2);

  arr.push(true);
  arr.push(1);
  arr.push(false);
  arr.push(0);
  t_true(arr.length() == 4, "11a");
  BoolArray* clone = arr.clone();
  t_true(clone->length() == 4, "11b");
  t_true(clone->get(0), "11c");
  t_true(clone->get(1), "11d");
  t_true(!clone->get(2), "11e");
  t_true(!clone->get(3), "11e");

  delete clone;
  OK("11");
}

void clone_stringarray_test() {
  String * x = new String("Hello");
  String * y = new String("World");
  String * z = new String("!");
  StringArray* arr = new StringArray(10);

  arr->push(x);
  arr->push(y);
  arr->push(z);
  t_true(arr->length() == 3, "12a");
  StringArray* clone = arr->clone();
  t_true(clone->length() == 3, "12b");
  t_true(clone->get(0)->equals(x), "12c");
  t_true(clone->get(1)->equals(y), "12d");
  t_true(clone->get(2)->equals(z), "12e");
  
  delete x;
  delete y;
  delete z;
  delete arr;

  String a("Hello");
  String b("World");
  String c("!");
  t_true(clone->get(0)->equals(&a), "12c");
  t_true(clone->get(1)->equals(&b), "12d");
  t_true(clone->get(2)->equals(&c), "12e");

  delete clone;
  OK("12");
}

void concat_string_test() {
  String string1("good");
  String string2("world");
  string1.concat("bye ");
  t_true(string1.size() == 8, "13a");
  t_true(strncmp(string1.c_str(), "goodbye ", string1.size()) == 0, "13b");
  string1.concat(&string2);
  t_true(string1.size() == 13, "13c");
  t_true(strncmp(string1.c_str(), "goodbye world", string1.size()) == 0, "13d");
  t_true(strncmp(string2.c_str(), "world", string2.size()) == 0, "13e");  
  string1.concat((size_t)243);
  t_true(string1.size() == 16, "13f");
  t_true(strncmp(string1.c_str(), "goodbye world243", string1.size()) == 0, "13g");
  string1.concat('a');
  t_true(string1.size() == 17, "13h");
  t_true(strncmp(string1.c_str(), "goodbye world243a", string1.size()) == 0, "13g");

  OK("13");
}


void basic_columnarray_test() {
  KV_Store kv(0);

  Column int_col('I', &kv);
  IntArray int_array;
  int_array.push(3);
  Key i_key("i", 0);
  int_col.push_back(&int_array, &i_key);

  Column double_col('D', &kv);
  DoubleArray d_array;
  d_array.push(232.3);
  Key d_key("d", 0);
  double_col.push_back(&d_array, &d_key);

  Column bool_col('B', &kv);
  BoolArray b_array;
  b_array.push(true);
  Key b_key("b", 0);
  bool_col.push_back(&b_array, &b_key);

  Column string_col('S', &kv);
  StringArray s_array;
  String str("hell");
  s_array.push(&str);
  Key s_key("s", 0);
  string_col.push_back(&s_array, &s_key);
  
  ColumnArray arr(10);
  arr.push(&int_col);
  arr.push(&double_col);
  arr.push(&bool_col);
  arr.push(&string_col);

  t_true(arr.length() == 4, "14a");
  // Test to show that a normal Column does not work, and must be IntColumn, DoubleColumn, etc.
  t_true(arr.get(0)->get_int(0) == 3, "14c");
  t_true(arr.get(1)->get_double(0) == (double)232.3, "14d");
  t_true(arr.get(2)->get_bool(0), "14e");
  t_true(arr.get(3)->get_string(0)->equals(&str), "14f");

  arr.clear();
  t_true(arr.length() == 0, "14h");

  OK("14");
}

void basic_keyarray_test() {
  KeyArray arr;
  size_t num_keys = 101;
  for (size_t ii = 0; ii < num_keys; ii++) {
    String key_string("key");
    key_string.concat(ii);
    Key temp_key(&key_string, ii);
    arr.push(&temp_key);
  }

  t_true(arr.length() == num_keys, "15a");
  for (size_t ii = 0; ii < num_keys; ii++) {
    String test_string("15b_");
    test_string.concat(ii);
    test_string.concat("_a");
    t_true(arr.get(ii)->get_node_index() == ii, test_string.c_str());
    
    String key_string("key");
    key_string.concat(ii);
    test_string.concat("_b");
    t_true(arr.get(ii)->get_key()->equals(&key_string), test_string.c_str());
  }

  arr.clear();
  t_true(arr.length() == 0, "15d");

  OK("15");
}

void array_test() {
  Array array('I');

  for (size_t ii = 0; ii < 100; ii++) {
    Payload p;
    p.i = ii;
    array.push(p);
  }

  for (size_t ii = 0; ii < 100; ii++) {
    assert(array.get(ii).i == ii);
  }

  assert(array.index_of(int_to_payload(10)) == 10);
  assert(array.replace(10, int_to_payload(0)).i == 10);
  assert(array.remove(10).i == 0);

  OK("16");
}

int main() {
  basic_object_test();
  basic_string_test();

  basic_boolarray_tests();
  basic_DoubleArray_tests();
  basic_intarray_test();
  basic_stringarray_test();

  complex_stringarray_test();

  object_array_test();

  clone_intarray_test();
  clone_DoubleArray_test();
  clone_boolarray_test();
  clone_stringarray_test();

  concat_string_test();
  basic_columnarray_test();
  basic_keyarray_test();
  array_test();

  exit(0);
}