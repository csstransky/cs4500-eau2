#include <stdlib.h>
#include <stdio.h>
#include "array.h"
#include "string.h"

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
  arr->pop();
  t_true(arr->length() == 2, "3b");
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
  arr->pop();
  t_true(arr->length() == 2, "4b");
  arr->clear();
  t_true(arr->length() == 0, "4c");

  delete arr;

  OK("4");
}

/** Tests pushing, popping, and length of Arrays */
void basic_floatarray_tests() {
  FloatArray * arr = new FloatArray(10);

  arr->push(1.5);
  arr->push(2.7);
  arr->push(3.9);
  t_true(arr->length() == 3, "5a");
  arr->pop();
  t_true(arr->length() == 2, "5b");
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
  arr->pop();
  t_true(arr->length() == 2, "6b");
  arr->clear();
  t_true(arr->length() == 0, "6c");

  delete arr;

  OK("3");
}

/** Tests more complex Array functions */
void complex_stringarray_test() {
  String * a = new String("This");
  String * b = new String("is going");
  String * c = new String("in a list");
  String * x = new String("Hello");
  String * y = new String("World");
  String * z = new String("!");
  StringArray * arr1 = new StringArray(10);
  StringArray * arr2 = new StringArray(10);

  arr1->push(a);
  arr1->push(b);
  arr1->push(c);
  arr2->push(x);
  arr2->push(y);
  arr2->push(z);
  String * copy_of_a = arr1->get(0);
  t_true(copy_of_a->equals(a), "7a");
  arr1->concat(arr2);
  t_true(arr1->length() == 6, "7b");
  t_true(arr2->length() == 3, "7c");
  t_true(arr1->index_of(y) == 4, "7d");
  t_true(arr2->index_of(y) == 1, "7e");
  t_true(arr2->index_of(z) == 2, "7f");
  arr2->remove(2);
  t_true(arr2->index_of(z) == -1, "7g");
  arr2->replace(1, z);
  t_true(arr2->index_of(z) == 1, "7h");
  t_true(arr2->index_of(y) == -1, "7i");
  StringArray * copy_of_arr1 = new StringArray(arr1);
  t_true(copy_of_arr1->equals(arr1), "7j");

  delete copy_of_arr1;
  delete arr2;
  delete arr1;
  delete z;
  delete y;
  delete x;
  delete c;
  delete b;
  delete a;

  OK("7");
}

/** Tests object array */
void object_array_test() {
  Object * a = new Object();
  Object * b = new Object();
  Object * c = new Object();
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
  delete arr1;

  OK("8");
}

int main() {
  basic_object_test();
  basic_string_test();

  basic_boolarray_tests();
  basic_floatarray_tests();
  basic_intarray_test();
  basic_stringarray_test();

  complex_stringarray_test();

  object_array_test();

  exit(0);
}