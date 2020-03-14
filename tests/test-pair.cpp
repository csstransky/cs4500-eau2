// Made by Kaylin Devchand and Cristian Stransky

#include <iostream>

#include "../src/helpers/object.h"  // Your file with the CwC declaration of Object
#include "../src/helpers/string.h"  // Your file with the String class
#include "../src/map/pair.h"
 
// This test class was added to allow for easier testing
class Test {
public:
  String* s;
  String* t;

  Test() {
    s = new String("Hello");
    t = new String("World");
  }

  ~Test() {
    delete s;
    delete t;
  }

  void FAIL() {   exit(1);    }
  void OK(const char* m) { std::cout << "OK: " << m << '\n'; }
  void t_true(bool p) { if (!p) FAIL(); }
  void t_false(bool p) { if (p) FAIL(); }

  void test_pair() {
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
    
    OK("test_pair");
  }

};

int main(int argc, char** argv) {
  Test* test = new Test();
  test->test_pair();
  delete test;
  return 0;
}

