#include <sys/wait.h>

#include "../src/dataframe/dataframe.h" 
#include "../src/kv_store/kd_store.h"
#include "../src/application/word_count_rowers.h"
#include "../src/networks/rendezvous_server.h"

#define GT_TRUE(a)   assert(a)
#define GT_FALSE(a)  assert(!a)
#define GT_EQUALS(a, b)   assert(a == b)

const int NUM_ROWS = 1000 * 1000;

/*******************************************************************************
 *  AddRower::
 *  A Rower to add a constant to every int.
 */
class AddRower : public Rower {
 public:
  int& sum_;

  AddRower(int& sum) : sum_(sum) { }

  ~AddRower() {
    
  }

    /** Return a copy of the object; nullptr is considered an error */
  Rower* clone() {
    int* sum = new int();
    return new AddRower(*sum);
  }

  /** Always returns true. Adds the num to each integer. */
  bool accept(Row& r) {
    for (size_t i = 0; i < r.width() - 1; i++) {
      switch (r.col_type(i))
      {
      case 'I': {
        int val = r.get_int(i);
        sum_ += val;
        break;
      }
      default:
        break;
      }
    }

    return true;
  }

  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  void join_delete(Rower* other) {
    int* other_sum = &dynamic_cast<AddRower*>(other)->sum_;
    sum_ += *other_sum;
    delete other_sum;
    delete other;
  }
 
};

void test() {
  Schema s("II");

  KV_Store kv(0);
  String c("c");
  
  DataFrame df(s, &c, &kv);
  Row  r(df.get_schema());
  for (size_t i = 0; i <  1000 * 1000; i++) {
    r.set(0,(int)i);
    r.set(1,(int)i+1);
    df.add_row(r);
  }
  GT_EQUALS(df.get_int((size_t)0,1), 1);
  printf("test passed!\n");
}

void max_test() {
    size_t large = 100;
    size_t small = 20;
    size_t maximum = max(large, small);
    GT_EQUALS(large, maximum);
    size_t maximum2 = max(small, large);
    GT_EQUALS(large, maximum2);
    GT_TRUE(maximum2 != small);

    printf("Max test passed!\n");
}

void min_test() {
    size_t large = 1000;
    size_t small = 1;
    size_t minimum = min(large, small);
    GT_EQUALS(small, minimum);
    size_t minimum2 = min(small, large);
    GT_EQUALS(small, minimum2);
    GT_TRUE(minimum2 != large);

    printf("Min test passed!\n");
}

void push_back_tests() {
    Column* int_column = new Column('I');
    Column* double_column = new Column('D');
    Column* bool_column = new Column('B');
    Column* string_column = new Column('S');

    int int_value = 2;
    double double_value = 2.2;
    bool bool_value = true;
    String* string_value = new String("lol");

    int_column->push_back(int_value);
    GT_EQUALS(int_column->size(), 1);
    double_column->push_back(double_value);
    GT_EQUALS(double_column->size(), 1);
    bool_column->push_back(bool_value);
    GT_EQUALS(bool_column->size(), 1);
    string_column->push_back(string_value);
    GT_EQUALS(string_column->size(), 1);

    delete int_column;
    delete double_column;
    delete bool_column;
    delete string_column;  
    delete string_value; 

    printf("Push back test passed!\n");
}

void get_type_tests() {
    Column* int_column = new Column('I');
    Column* double_column = new Column('D');
    Column* bool_column = new Column('B');
    Column* string_column = new Column('S');

    GT_EQUALS(int_column->get_type(), 'I');
    GT_EQUALS(double_column->get_type(), 'D');
    GT_EQUALS(bool_column->get_type(), 'B');
    GT_EQUALS(string_column->get_type(), 'S');

    delete int_column;
    delete double_column;
    delete bool_column;
    delete string_column; 

    printf("Get type test passed!\n"); 
}

void size_tests() {
    KV_Store kv(0);
    String c("c");
  
    Column* int_column = new Column('I', &kv, &c, 0);
    Column* double_column = new Column('D', &kv, &c, 1);
    Column* bool_column = new Column('B', &kv, &c, 2);
    Column* string_column = new Column('S', &kv, &c, 3);

    int int_value = 2;
    double double_value = 2.2;
    bool bool_value = true;
    String* string_value = new String("lol");

    for (size_t ii = 0; ii < 400; ii++) {
        int_column->push_back(int_value);
        GT_EQUALS(int_column->size(), ii + 1);
    }
    for (size_t ii = 0; ii < 400; ii++) {
        double_column->push_back(double_value);
        GT_EQUALS(double_column->size(), ii + 1);
    }
    for (size_t ii = 0; ii < 400; ii++) {
        bool_column->push_back(bool_value);
        GT_EQUALS(bool_column->size(), ii + 1);
    }
    for (size_t ii = 0; ii < 400; ii++) {
        string_column->push_back(string_value);
        GT_EQUALS(string_column->size(), ii + 1);
    }

    delete int_column;
    delete double_column;
    delete bool_column;
    delete string_column;  
    delete string_value; 

    printf("Size test passed!\n");
}

void int_column_set_tests() {
    Column* int_column = new Column('I');
    int_column->push_back(23);
    int_column->push_back(42);
    int_column->push_back(5);
    int_column->push_back(5234);
    int_column->push_back(2342);
    int_column->push_back(2);
    int_column->push_back(188);
    GT_EQUALS(int_column->size(), 7);
    GT_EQUALS(int_column->get_int(0), 23);
    GT_EQUALS(int_column->get_int(3), 5234);
    GT_EQUALS(int_column->get_int(4), 2342);
    GT_EQUALS(int_column->get_int(5), 2);
    GT_EQUALS(int_column->get_int(6), 188);

    delete int_column;

    printf("Int column set test passed!\n");
}

void double_column_set_tests() {
    Column* double_column = new Column('D');
    double_column->push_back(23.12);
    double_column->push_back(42.3);
    double_column->push_back(5.1);
    double_column->push_back((double)5234);
    double_column->push_back(2342.33);
    double_column->push_back(2.1);
    double_column->push_back((double)188);
    GT_EQUALS(double_column->size(), 7);
    GT_EQUALS(double_column->get_double(0), (double)23.12);
    GT_EQUALS(double_column->get_double(3), (double)5234);
    GT_EQUALS(double_column->get_double(4), (double)2342.33);
    GT_EQUALS(double_column->get_double(5), (double)2.1);
    GT_EQUALS(double_column->get_double(6), (double)188);

    delete double_column;

    printf("Double column set test passed!\n");
}

void bool_column_set_tests() {
    Column* bool_column = new Column('B');
    bool_column->push_back((bool)0);
    bool_column->push_back(false);
    bool_column->push_back((bool)1);
    bool_column->push_back(true);
    bool_column->push_back(true);
    bool_column->push_back(false);
    bool_column->push_back(false);
    GT_EQUALS(bool_column->size(), 7);
    GT_EQUALS(bool_column->get_bool(0), 0);
    GT_EQUALS(bool_column->get_bool(3), true);
    GT_EQUALS(bool_column->get_bool(4), 1);
    GT_EQUALS(bool_column->get_bool(5), 0);
    GT_EQUALS(bool_column->get_bool(6), false);

    delete bool_column;

    printf("Bool column set test passed!\n");
}

void string_column_set_tests() {
    String* string1 = new String("lol");
    String* string2 = new String("BIG LOL");
    String* string3 = new String("Someone end this alreayd");
    Column* string_column = new Column('S');
    string_column->push_back(string1);
    string_column->push_back(string2);
    string_column->push_back(string3);
    string_column->push_back(string3);
    GT_EQUALS(string_column->size(), 4);
    GT_TRUE(string_column->get_string(0)->equals(string1));
    GT_TRUE(string_column->get_string(1)->equals(string2));
    GT_TRUE(string_column->get_string(2)->equals(string3));
    GT_TRUE(string_column->get_string(3)->equals(string3));

    delete string1;
    delete string2;
    delete string3;
    delete string_column;

    printf("String column set test passed!\n");
}

void schema_constructor_tests() {
    Schema* schema1 = new Schema();
    GT_EQUALS(schema1->width(), 0);
    GT_EQUALS(schema1->length(), 0);

    const char* types = "ISBDDBS";
    Schema* schema2 = new Schema(types);
    GT_EQUALS(schema2->width(), 7);
    GT_EQUALS(schema2->length(), 0);
    for (size_t ii = 0; ii < schema2->width(); ii++) {
        GT_EQUALS(schema2->col_type(ii), types[ii]);
    }
    
    Schema* schema3 = new Schema(*schema2);
    GT_EQUALS(schema3->width(), 7);
    GT_EQUALS(schema3->length(), 0);

    for (size_t ii = 0; ii < schema3->width(); ii++) {
        GT_EQUALS(schema3->col_type(ii), types[ii]);
    }

    // Testing to see that the copied Schema is not linked to its original
    schema3->add_column('S');
    schema3->add_row();
    GT_EQUALS(schema3->col_type(7), 'S');
    GT_EQUALS(schema3->width(), 8);
    GT_EQUALS(schema2->width(), 7);
    GT_EQUALS(schema3->length(), 1);
    GT_EQUALS(schema2->length(), 0);

    delete schema1;
    delete schema2;
    delete schema3;

    printf("Schema constructor test passed!\n");
}

void schema_add_column_tests() {
    Schema* schema1 = new Schema();
    for (size_t ii = 0; ii < 100; ii++) {
        schema1->add_column('S');
    }
    GT_EQUALS(schema1->width(), 100);
    GT_EQUALS(schema1->col_type(47), 'S');
    
    String* name = new String("hi");
    schema1->add_column('D');
    GT_EQUALS(schema1->width(), 101);
    GT_EQUALS(schema1->length(), 0);
    GT_EQUALS(schema1->col_type(100), 'D');

    delete schema1;
    delete name;

    printf("Schema add column test passed!\n");
}

void schema_add_row_tests() {
    Schema* schema1 = new Schema();
    for (size_t ii = 0; ii < 100; ii++) {
        schema1->add_row();
    }
    GT_EQUALS(schema1->length(), 100);
    
    String* name = new String("hi");
    schema1->add_row();
    GT_EQUALS(schema1->length(), 101);
    GT_EQUALS(schema1->width(), 0);

    delete schema1;
    delete name;

    printf("Schema add row test passed!\n");
}


void test_row_width() {
  Schema* s = new Schema("II");
  Row* row = new Row(*s);
  GT_EQUALS(row->width(), 2);

  delete row;
  delete s;

  printf("Row width test passed!\n");
}

void test_col_type() {
  Schema* s = new Schema("IDSB");
  Row* row = new Row(*s);
  GT_EQUALS(row->col_type(0), 'I');
  GT_EQUALS(row->col_type(1), 'D');
  GT_EQUALS(row->col_type(2), 'S');
  GT_EQUALS(row->col_type(3), 'B');

  delete row;
  delete s;

  printf("Col type test passed!\n");
}

void test_set_get() {
  Schema* s = new Schema("IDSB");
  Row* row = new Row(*s);
  String* hi = new String("hi");
  row->set(0, 4);
  row->set(1, (double)3.2);
  row->set(2, hi);
  row->set(3, (bool)0);
  GT_EQUALS(row->get_int(0), 4);
  GT_EQUALS(row->get_double(1), (double)3.2);
  GT_EQUALS(row->get_string(2)->equals(hi), 1);
  GT_EQUALS(row->get_bool(3), 0);

  delete s;
  delete row;
  delete hi;

  printf("Set get test passed!\n");
}

void test_map_add() {
  KV_Store kv(0);
  String c("c");
  Schema* s = new Schema("IIDI");
  DataFrame* df = new DataFrame(*s, &c, &kv);
  Row* r = new Row(df->get_schema());
  int actual = 0;
  AddRower* rower = new AddRower(actual);

  for (size_t i = 0; i <  NUM_ROWS; i++) {
    r->set(0,(int)i);
    r->set(1,(int)i+1);
    r->set(2, (double)i);
    df->add_row(*r);
  }

  df->map(*rower);

  int expected = 0;

  for (size_t i = 0; i < NUM_ROWS; i++) {
    expected += i+i+1;
  }

  GT_TRUE(actual == expected);

  for (size_t i = 0; i < NUM_ROWS; i++) {
    GT_EQUALS(df->get_int((size_t)0,i), i);
    GT_EQUALS(df->get_int((size_t)1,i), i+1);
    GT_EQUALS(df->get_double((size_t)2,i), (double)i);
  }

  delete s;
  delete df;
  delete r;
  delete rower;

  printf("Map add test passed!\n");
  
}

void test_get_schema() {
  KV_Store kv(0);
  String c("c");
  Schema* s1 = new Schema("IDSB");
  DataFrame* df = new DataFrame(*s1, &c, &kv);

  Schema s2 = df->get_schema();

  for (int i = 0; i < 4; i++) {
    GT_EQUALS(s1->col_type(i), s2.col_type(i));
  }

  delete s1;
  delete df;

  printf("Get schema test passed!\n");
}

void test_add_column() {
  KV_Store kv(0);
  String c("c");
  Schema s1("IDSB");
  DataFrame df(s1, &c, &kv);
  String* hi = new String("hi");
  String* hello = new String("hello");
  String* h = new String("h");

  GT_EQUALS(df.ncols(), 4);
  GT_EQUALS(df.nrows(), 0);

  Column* c_int = new Column('I');
  c_int->push_back(1);
  c_int->push_back(2);
  c_int->push_back(3);
  c_int->push_back(4);
  Column* c_double = new Column('D');
  c_double->push_back((double)1.2);
  c_double->push_back((double)2.2);
  c_double->push_back((double)3.2);
  c_double->push_back((double)4.2);
  Column* c_string = new Column('S');
  c_string->push_back(hi);
  c_string->push_back(hi);
  c_string->push_back(hi);
  c_string->push_back(hi);
  Column* c_bool = new Column('B');
  c_bool->push_back((bool)0);
  c_bool->push_back((bool)1);
  c_bool->push_back((bool)0);
  c_bool->push_back((bool)1);

  df.add_column(c_int);
  df.add_column(c_double);
  df.add_column(c_string);
  df.add_column(c_bool);

  GT_EQUALS(df.ncols(), 8);
  GT_EQUALS(df.nrows(), 4);

  for (int i = 0; i < 4; i++) {
    GT_EQUALS(df.get_int(0, i), DEFAULT_INT_VALUE);
    GT_EQUALS(df.get_double(1, i), DEFAULT_DOUBLE_VALUE);
    GT_TRUE(df.get_string(2, i)->equals(&DEFAULT_STRING_VALUE));
    GT_EQUALS(df.get_bool(3, i), DEFAULT_BOOL_VALUE); 
  }

  for (int i = 0; i < 4; i++) {
    GT_EQUALS(df.get_int(4, i), i+1);
    GT_EQUALS(df.get_double(5, i), (double)(i + 1.2));
    GT_TRUE(df.get_string(6, i)->equals(hi));
    GT_EQUALS(df.get_bool(7, i), i % 2); 
  }
  
  for (int i = 0; i < 100; i++) {
    df.add_column(c_bool);
  }

  GT_EQUALS(df.ncols(), 108);
  GT_EQUALS(df.nrows(), 4);
  

  delete h;
  delete hi;
  delete hello;
  delete c_int;
  delete c_bool;
  delete c_double;
  delete c_string;

  printf("Add column test passed!\n");
}

void dataframe_constructor_tests() {
  KV_Store kv(0);
  String c("c");
  Schema schema1("IDSBSB");
  DataFrame* dataframe1 = new DataFrame(schema1, &c, &kv);
  GT_EQUALS(dataframe1->ncols(), 6);
  GT_EQUALS(dataframe1->nrows(), 0);

  String c_new("c_new");
  DataFrame* dataframe2 = new DataFrame(*dataframe1, &c_new);
  Column* int_column = new Column('I');
  GT_EQUALS(dataframe2->ncols(), 6);
  GT_EQUALS(dataframe2->nrows(), 0);
  
  // Testing to see that the copy is not linked to the original
  dataframe2->add_column(int_column);
  GT_EQUALS(dataframe1->ncols(), 6);
  GT_EQUALS(dataframe1->nrows(), 0);
  GT_EQUALS(dataframe2->ncols(), 7);
  GT_EQUALS(dataframe2->nrows(), 0);

  delete dataframe1;
  delete dataframe2;
  delete int_column;

  printf("Dataframe constructor test passed!\n");
}

void dataframe_getters_tests() { 
  KV_Store kv(0);
  String c("c");
  Schema s1("");
  DataFrame df(s1, &c, &kv);
  String* hi = new String("hi");
  String* hello = new String("hello");
  String* h = new String("h");

  GT_EQUALS(df.ncols(), 0);
  GT_EQUALS(df.nrows(), 0);

  Column* c_int = new Column('I');
  c_int->push_back(1);
  c_int->push_back(3);
  c_int->push_back(4);
  c_int->push_back(2);
  Column* c_double = new Column('D');
  c_double->push_back((double)1.2);
  c_double->push_back((double)3.2);
  c_double->push_back((double)2);
  c_double->push_back((double)1);
  Column* c_string = new Column('S');
  c_string->push_back(hi);
  c_string->push_back(hello);
  c_string->push_back(nullptr);
  c_string->push_back(hi);
  c_string->push_back(h);
  Column* c_bool = new Column('B');
  c_bool->push_back((bool)0);
  c_bool->push_back((bool)1);
  c_bool->push_back((bool)1);

  df.add_column(c_int);
  df.add_column(c_double);
  df.add_column(c_string);
  df.add_column(c_bool);

  GT_EQUALS(df.get_int(0, 0), 1);
  GT_EQUALS(df.get_int(0, 1), 3);
  GT_EQUALS(df.get_int(0, 2), 4);
  GT_EQUALS(df.get_int(0, 3), 2);

  GT_EQUALS(df.get_double(1, 0), (double)1.2);
  GT_EQUALS(df.get_double(1, 1), (double)3.2);
  GT_EQUALS(df.get_double(1, 2), (double)2);
  GT_EQUALS(df.get_double(1, 3), (double)1);

  GT_TRUE(df.get_string(2, 0)->equals(hi));
  GT_TRUE(df.get_string(2, 1)->equals(hello));
  GT_TRUE(df.get_string(2, 2)->equals(&DEFAULT_STRING_VALUE));
  GT_TRUE(df.get_string(2, 3)->equals(hi));
  GT_TRUE(df.get_string(2, 4)->equals(h));

  GT_EQUALS(df.get_bool(3, 0), false);
  GT_EQUALS(df.get_bool(3, 1), 1);
  GT_EQUALS(df.get_bool(3, 2), true);

  // Test that this array grew to fit the row size
  GT_EQUALS(df.get_bool(3, 3), DEFAULT_BOOL_VALUE);

  // Test that the rest of the arrays grew from the extra string array rows
  GT_EQUALS(df.get_int(0, 4), DEFAULT_INT_VALUE);
  GT_EQUALS(df.get_double(1, 4), DEFAULT_DOUBLE_VALUE);
  GT_EQUALS(df.get_bool(3, 4), DEFAULT_BOOL_VALUE);

  delete h;
  delete hi;
  delete hello;
  delete c_int;
  delete c_bool;
  delete c_double;
  delete c_string;

  printf("Dataframe set get test passed!\n");
}

void dataframe_fill_row_tests() { 
  KV_Store kv(0);
  String c("c");
  Schema s1("");
  DataFrame df(s1, &c, &kv);
  String* hi = new String("hi");
  String* hello = new String("hello");
  String* h = new String("h");

  GT_EQUALS(df.ncols(), 0);
  GT_EQUALS(df.nrows(), 0);

  Column* c_int = new Column('I');
  c_int->push_back(1);
  c_int->push_back(3);
  c_int->push_back(4);
  c_int->push_back(2);
  Column* c_double = new Column('D');
  c_double->push_back((double)1.2);
  c_double->push_back((double)3.2);
  c_double->push_back((double)2);
  c_double->push_back((double)1);
  Column* c_string = new Column('S');
  c_string->push_back(hi);
  c_string->push_back(hello);
  c_string->push_back(nullptr);
  c_string->push_back(hi);
  c_string->push_back(h);
  Column* c_bool = new Column('B');
  c_bool->push_back((bool)0);
  c_bool->push_back((bool)1);
  c_bool->push_back((bool)1);

  df.add_column(c_int);
  df.add_column(c_double);
  df.add_column(c_string);
  df.add_column(c_bool);

  Row* row1 = new Row(df.get_schema());
  Row* row2 = new Row(df.get_schema());
  Row* row3 = new Row(df.get_schema());
  Row* row4 = new Row(df.get_schema());
  Row* row5 = new Row(df.get_schema());

  df.fill_row(0, *row1);
  df.fill_row(1, *row2);
  df.fill_row(2, *row3);
  df.fill_row(3, *row4);
  df.fill_row(4, *row5);

  GT_EQUALS(row1->get_int(0), 1);
  GT_EQUALS(row2->get_int(0), 3);
  GT_EQUALS(row3->get_int(0), 4);
  GT_EQUALS(row4->get_int(0), 2);
  GT_EQUALS(row5->get_int(0), 0);

  GT_EQUALS(row1->get_double(1), (double)1.2);
  GT_EQUALS(row2->get_double(1), (double)3.2);
  GT_EQUALS(row3->get_double(1), (double)2);
  GT_EQUALS(row4->get_double(1), (double)1);
  GT_EQUALS(row5->get_double(1), 0);

  GT_TRUE(row1->get_string(2)->equals(hi));
  GT_TRUE(row2->get_string(2)->equals(hello));
  GT_TRUE(row3->get_string(2)->equals(&DEFAULT_STRING_VALUE));
  GT_TRUE(row4->get_string(2)->equals(hi));
  GT_TRUE(row5->get_string(2)->equals(h));

  GT_EQUALS(row1->get_bool(3), false);
  GT_EQUALS(row2->get_bool(3), 1);
  GT_EQUALS(row3->get_bool(3), true);
  GT_EQUALS(row4->get_bool(3), false);
  GT_EQUALS(row5->get_bool(3), false);

  delete h;
  delete hi;
  delete hello;
  delete c_int;
  delete c_bool;
  delete c_double;
  delete c_string;
  delete row1;
  delete row2;
  delete row3; 
  delete row4;
  delete row5;

  printf("Dataframe fill row test passed!\n");
}

void dataframe_add_row_tests() { 
  KV_Store kv(0);
  String c("c");
  Schema s1("");
  DataFrame df(s1, &c, &kv);
  String* hi = new String("hi");
  String* hello = new String("hello");
  String* h = new String("h");

  GT_EQUALS(df.ncols(), 0);
  GT_EQUALS(df.nrows(), 0);

  Column* c_int = new Column('I');
  c_int->push_back(1);
  c_int->push_back(3);
  c_int->push_back(4);
  c_int->push_back(2);
  Column* c_double = new Column('D');
  c_double->push_back((double)1.2);
  c_double->push_back((double)3.2);
  c_double->push_back((double)2);
  c_double->push_back((double)1);
  Column* c_string = new Column('S');
  c_string->push_back(hi);
  c_string->push_back(hello);
  c_string->push_back(nullptr);
  c_string->push_back(hi);
  c_string->push_back(h);
  Column* c_bool = new Column('B');
  c_bool->push_back((bool)0);
  c_bool->push_back((bool)1);
  c_bool->push_back((bool)1);

  df.add_column(c_int);
  df.add_column(c_double);
  df.add_column(c_string);
  df.add_column(c_bool);

  Row* row1 = new Row(df.get_schema());
  Row* row2 = new Row(df.get_schema());
  Row* row3 = new Row(df.get_schema());
  Row* row4 = new Row(df.get_schema());
  Row* row5 = new Row(df.get_schema());

  df.fill_row(0, *row1);
  df.fill_row(1, *row2);
  df.fill_row(2, *row3);
  df.fill_row(3, *row4);
  df.fill_row(4, *row5);

  for (size_t ii = 0; ii < 1000; ii++) {
    df.add_row(*row1);
    df.add_row(*row2);
    df.add_row(*row3);
    df.add_row(*row4);
    df.add_row(*row5);
  }
  GT_EQUALS(df.ncols(), 4);
  // initial 5 rows from the columns, and then the iterated 5000
  GT_EQUALS(df.nrows(), 5000 + 5);

  for (size_t ii = 0; ii < 5000; ii += 5) {
    GT_EQUALS(df.get_int(0, ii), 1);
    GT_EQUALS(df.get_int(0, ii + 1), 3);
    GT_EQUALS(df.get_int(0, ii + 2), 4);
    GT_EQUALS(df.get_int(0, ii + 3), 2);
    GT_EQUALS(df.get_int(0, ii + 4), 0);

    GT_EQUALS(df.get_double(1, ii), (double)1.2);
    GT_EQUALS(df.get_double(1, ii + 1), (double)3.2);
    GT_EQUALS(df.get_double(1, ii + 2), (double)2);
    GT_EQUALS(df.get_double(1, ii + 3), (double)1);
    GT_EQUALS(df.get_double(1, ii + 4), 0);

    GT_TRUE(df.get_string(2, ii)->equals(hi));
    GT_TRUE(df.get_string(2, ii + 1)->equals(hello));
    GT_TRUE(df.get_string(2, ii + 2)->equals(&DEFAULT_STRING_VALUE));
    GT_TRUE(df.get_string(2, ii + 3)->equals(hi));
    GT_TRUE(df.get_string(2, ii + 4)->equals(h));

    GT_EQUALS(df.get_bool(3, ii), false);
    GT_EQUALS(df.get_bool(3, ii + 1), 1);
    GT_EQUALS(df.get_bool(3, ii + 2), true);
    GT_EQUALS(df.get_bool(3, ii + 3), false);
    GT_EQUALS(df.get_bool(3, ii + 4), 0);
  }

  // Show that the row is not directly linked to the dataframe
  row1->set(0, 1000);
  row1->set(3, true);
  GT_EQUALS(row1->get_int(0), 1000);
  GT_EQUALS(row1->get_bool(3), true);
  GT_EQUALS(df.get_int(0, 0), 1);
  GT_EQUALS(df.get_bool(3, 0), false);

  delete h;
  delete hi;
  delete hello;
  delete c_int;
  delete c_bool;
  delete c_double;
  delete c_string;
  delete row1;
  delete row2;
  delete row3; 
  delete row4;
  delete row5;

  printf("Dataframe add row test passed!\n");
}

void test_pmap_add() {
  KV_Store kv(0);
  String c("c");
  Schema s("IIDI");
  DataFrame df(s, &c, &kv);
  Row  r(df.get_schema());
  int actual = 0;
  AddRower rower(actual);

  for (size_t i = 0; i < NUM_ROWS; i++) {
    r.set(0,(int)i);
    r.set(1,(int)i+1);
    r.set(2, (double)i);
    df.add_row(r);
  }

  df.pmap(rower);

  int expected = 0;

  for (size_t i = 0; i < NUM_ROWS; i++) {
    expected += i+i+1;
  }

  GT_TRUE(actual == expected);

  for (size_t i = 0; i < NUM_ROWS; i++) {
    GT_EQUALS(df.get_int((size_t)0,i), i);
    GT_EQUALS(df.get_int((size_t)1,i), i+1);
    GT_EQUALS(df.get_double((size_t)2,i), (double)i);
  }

  printf("Pmap add test passed!\n");
  
}

void test_from_array_int() {
  size_t num = 500;
  int array[num];

  for (size_t i = 0; i < num; i++) {
    array[i] = i;
  }

  Key k("k", 0);
  KD_Store kd(0);
  DataFrame* df = DataFrame::from_array(&k, &kd, num, array);

  assert(df->ncols() == 1);
  assert(df->nrows() == num);
  for (size_t i = 0; i < num; i++) {
    assert(df->get_int(0,i) == array[i]);
  }

  DataFrame* df2 = kd.get(&k);

  assert(df2->ncols() == 1);
  assert(df2->nrows() == num);
  for (size_t i = 0; i < num; i++) {
    assert(df2->get_int(0,i) == array[i]);
  }

  delete df;
  delete df2;

  printf("Dataframe from int array tests pass!\n");
}

void test_from_array_double() {
  size_t num = 500;
  double array[num];

  for (size_t i = 0; i < num; i++) {
    array[i] = i + 0.2;
  }

  Key k("k", 0);
  KD_Store kd(0);
  DataFrame* df = DataFrame::from_array(&k, &kd, num, array);

  assert(df->ncols() == 1);
  assert(df->nrows() == num);
  for (size_t i = 0; i < num; i++) {
    assert(df->get_double(0,i) == array[i]);
  }

  DataFrame* df2 = kd.get(&k);

  assert(df2->ncols() == 1);
  assert(df2->nrows() == num);
  for (size_t i = 0; i < num; i++) {
    assert(df2->get_double(0,i) == array[i]);
  }

  delete df;
  delete df2;

  printf("Dataframe from double array tests pass!\n");
}

void test_from_array_bool() {
  size_t num = 500;
  bool array[num];

  for (size_t i = 0; i < num; i++) {
    array[i] = i % 2;
  }

  Key k("k", 0);
  KD_Store kd(0);
  DataFrame* df = DataFrame::from_array(&k, &kd, num, array);

  assert(df->ncols() == 1);
  assert(df->nrows() == num);
  for (size_t i = 0; i < num; i++) {
    assert(df->get_bool(0,i) == array[i]);
  }

  DataFrame* df2 = kd.get(&k);

  assert(df2->ncols() == 1);
  assert(df2->nrows() == num);
  for (size_t i = 0; i < num; i++) {
    assert(df2->get_bool(0,i) == array[i]);
  }

  delete df;
  delete df2;

  printf("Dataframe from bool array tests pass!\n");
}

void test_from_array_string() {
  size_t num = 500;
  String* array[num];

  for (size_t i = 0; i < num; i++) {
    array[i] = new String("hi");
  }

  Key k("k", 0);
  KD_Store kd(0);
  DataFrame* df = DataFrame::from_array(&k, &kd, num, array);

  assert(df->ncols() == 1);
  assert(df->nrows() == num);
  for (size_t i = 0; i < num; i++) {
    assert(df->get_string(0,i)->equals(array[i]));
  }

  DataFrame* df2 = kd.get(&k);

  assert(df2->ncols() == 1);
  assert(df2->nrows() == num);
  for (size_t i = 0; i < num; i++) {
    assert(df2->get_string(0,i)->equals(array[i]));
  }

  delete df;
  delete df2;

  for (size_t i = 0; i < num; i++) {
    delete array[i];
  }

  printf("Dataframe from string array tests pass!\n");
}

void test_from_file() {
  char* path = const_cast<char*>("data/doc.txt");

  Key k("k", 0);
  KD_Store kd(0);
  DataFrame* dataframe = DataFrame::from_file(&k, &kd, path);

  String* schema_types = dataframe->get_schema().types_;
  assert(strcmp("SISSBBBB", schema_types->c_str()) == 0);
    
  String string0("0hi");
  assert(dataframe->get_string(0, 0)->equals(&string0));
  assert(dataframe->get_int(1, 0) == 11);
  String string1("true");
  assert(dataframe->get_string(2, 0)->equals(&string1));
  assert(dataframe->get_string(3, 0)->equals(&DEFAULT_STRING_VALUE));
  assert(!dataframe->get_bool(4, 0));
  assert(!dataframe->get_bool(5, 0));
  assert(!dataframe->get_bool(6, 0));
  assert(!dataframe->get_bool(7, 0));

  String string2("1yellow");
  assert(dataframe->get_string(0, 1)->equals(&string2));
  assert(dataframe->get_int(1, 1) == 10);
  String string3("false");
  assert(dataframe->get_string(2, 1)->equals(&string3));
  String string4("string");
  assert(dataframe->get_string(3, 1)->equals(&string4));
  assert(!dataframe->get_bool(4, 1));
  assert(!dataframe->get_bool(5, 1));
  assert(!dataframe->get_bool(6, 1));
  assert(!dataframe->get_bool(7, 1));

  String string5("212");
  assert(dataframe->get_string(0, 2)->equals(&string5));
  assert(dataframe->get_int(1, 2) == 0);
  String string6("true");
  assert(dataframe->get_string(2, 2)->equals(&string6));
  String string6_2("");
  assert(dataframe->get_string(3, 2)->equals(&string6_2));
  assert(!dataframe->get_bool(4, 2));
  assert(!dataframe->get_bool(5, 2));
  assert(!dataframe->get_bool(6, 2));
  assert(!dataframe->get_bool(7, 2));

  String string7("3vroom");
  assert(dataframe->get_string(0, 3)->equals(&string7));
  assert(dataframe->get_int(1, 3) == 172);
  String string8("hi");
  assert(dataframe->get_string(2, 3)->equals(&string8));
  String string9("hihi");
  assert(dataframe->get_string(3, 3)->equals(&string9));
  assert(!dataframe->get_bool(4, 3));
  assert(!dataframe->get_bool(5, 3));
  assert(!dataframe->get_bool(6, 3));
  assert(!dataframe->get_bool(7, 3));
  
  String string10("4hello");
  assert(dataframe->get_string(0, 4)->equals(&string10));
  assert(dataframe->get_int(1, 4) == 12);
  String string11("false");
  assert(dataframe->get_string(2, 4)->equals(&string11));
  String string12("");
  assert(dataframe->get_string(3, 4)->equals(&string12));
  assert(dataframe->get_bool(4, 4));
  assert(!dataframe->get_bool(5, 4));
  assert(!dataframe->get_bool(6, 4));
  assert(!dataframe->get_bool(7, 4));

  String string13("5\"quote checl\"");
  assert(dataframe->get_string(0, 5)->equals(&string13));
  assert(dataframe->get_int(1, 5) == 0);
  String string14("\"I guess this'll\"worktoo");
  assert(dataframe->get_string(2, 5)->equals(&string14));
  assert(dataframe->get_string(3, 5)->equals(&DEFAULT_STRING_VALUE));
  assert(!dataframe->get_bool(4, 5));
  assert(!dataframe->get_bool(5, 5));
  assert(!dataframe->get_bool(6, 5));
  assert(!dataframe->get_bool(7, 5));

  String string16("");
  assert(dataframe->get_string(0, 6)->equals(&string16));
  assert(dataframe->get_int(1, 6) == 0);
  String string17("reallyno");
  assert(dataframe->get_string(2, 6)->equals(&string17));
  assert(dataframe->get_string(3, 6)->equals(&DEFAULT_STRING_VALUE));
  assert(!dataframe->get_bool(4, 6));
  assert(!dataframe->get_bool(5, 6));
  assert(!dataframe->get_bool(6, 6));
  assert(!dataframe->get_bool(7, 6));

  delete dataframe;

  dataframe = kd.get(&k);

  schema_types = dataframe->get_schema().types_;
  assert(strcmp("SISSBBBB", schema_types->c_str()) == 0);
    
  assert(dataframe->get_string(0, 0)->equals(&string0));
  assert(dataframe->get_int(1, 0) == 11);
  assert(dataframe->get_string(2, 0)->equals(&string1));
  assert(dataframe->get_string(3, 0)->equals(&DEFAULT_STRING_VALUE));
  assert(!dataframe->get_bool(4, 0));
  assert(!dataframe->get_bool(5, 0));
  assert(!dataframe->get_bool(6, 0));
  assert(!dataframe->get_bool(7, 0));

  assert(dataframe->get_string(0, 1)->equals(&string2));
  assert(dataframe->get_int(1, 1) == 10);
  assert(dataframe->get_string(2, 1)->equals(&string3));
  assert(dataframe->get_string(3, 1)->equals(&string4));
  assert(!dataframe->get_bool(4, 1));
  assert(!dataframe->get_bool(5, 1));
  assert(!dataframe->get_bool(6, 1));
  assert(!dataframe->get_bool(7, 1));

  assert(dataframe->get_string(0, 2)->equals(&string5));
  assert(dataframe->get_int(1, 2) == 0);
  assert(dataframe->get_string(2, 2)->equals(&string6));
  assert(dataframe->get_string(3, 2)->equals(&string6_2));
  assert(!dataframe->get_bool(4, 2));
  assert(!dataframe->get_bool(5, 2));
  assert(!dataframe->get_bool(6, 2));
  assert(!dataframe->get_bool(7, 2));

  assert(dataframe->get_string(0, 3)->equals(&string7));
  assert(dataframe->get_int(1, 3) == 172);
  assert(dataframe->get_string(2, 3)->equals(&string8));
  assert(dataframe->get_string(3, 3)->equals(&string9));
  assert(!dataframe->get_bool(4, 3));
  assert(!dataframe->get_bool(5, 3));
  assert(!dataframe->get_bool(6, 3));
  assert(!dataframe->get_bool(7, 3));
  
  assert(dataframe->get_string(0, 4)->equals(&string10));
  assert(dataframe->get_int(1, 4) == 12);
  assert(dataframe->get_string(2, 4)->equals(&string11));
  assert(dataframe->get_string(3, 4)->equals(&string12));
  assert(dataframe->get_bool(4, 4));
  assert(!dataframe->get_bool(5, 4));
  assert(!dataframe->get_bool(6, 4));
  assert(!dataframe->get_bool(7, 4));

  assert(dataframe->get_string(0, 5)->equals(&string13));
  assert(dataframe->get_int(1, 5) == 0);
  assert(dataframe->get_string(2, 5)->equals(&string14));
  assert(dataframe->get_string(3, 5)->equals(&DEFAULT_STRING_VALUE));
  assert(!dataframe->get_bool(4, 5));
  assert(!dataframe->get_bool(5, 5));
  assert(!dataframe->get_bool(6, 5));
  assert(!dataframe->get_bool(7, 5));

  assert(dataframe->get_string(0, 6)->equals(&string16));
  assert(dataframe->get_int(1, 6) == 0);
  assert(dataframe->get_string(2, 6)->equals(&string17));
  assert(dataframe->get_string(3, 6)->equals(&DEFAULT_STRING_VALUE));
  assert(!dataframe->get_bool(4, 6));
  assert(!dataframe->get_bool(5, 6));
  assert(!dataframe->get_bool(6, 6));
  assert(!dataframe->get_bool(7, 6));

  delete dataframe;

  printf("Dataframe from file tests pass!\n");
}

void test_from_scalar() {
  Key k_int("int", 0);
  KD_Store kd(0);
  DataFrame* df = DataFrame::from_scalar(&k_int, &kd, 1);
  assert(df->get_int(0,0) == 1);
  delete df;

  Key k_double("double", 0);
  df = DataFrame::from_scalar(&k_double, &kd, (double)1.1);
  assert(df->get_double(0,0) == (double)1.1);
  delete df;

  Key k_bool("bool", 0);
  df = DataFrame::from_scalar(&k_bool, &kd, (bool)1);
  assert(df->get_bool(0,0) == true);
  delete df;

  Key k_string("string", 0);
  String s("s");
  df = DataFrame::from_scalar(&k_string, &kd, &s);
  assert(df->get_string(0,0)->equals(&s));
  delete df;

  df = kd.get(&k_int);
  assert(df->get_int(0,0) == 1);
  delete df;

  df = kd.get(&k_double);
  assert(df->get_double(0,0) == 1.1);
  delete df;

  df = kd.get(&k_bool);
  assert(df->get_bool(0,0) == true);
  delete df;

  df = kd.get(&k_string);
  assert(df->get_string(0,0)->equals(&s));
  delete df;

  printf("Dataframe from scalar tests pass!\n");
}

void test_from_rower_summer() {
  SIMap map;
  char buf[20];
  size_t count = 300;
  for (size_t ii = 0; ii < count; ii++) {
    snprintf(buf, 20, "s_%zu", ii);
    String s(buf);
    Num n(ii);
    map.put(&s, &n);
  }

  Summer cnt(map); 
  Key summer("summer", 0);
  KD_Store kd(0);
  DataFrame* df = DataFrame::from_rower(&summer, &kd, "SI", cnt);

  assert(df->ncols() == 2);
  assert(df->nrows() == count);
  for (size_t ii = 0; ii < count; ii++) {
    int c = df->get_int(1, ii);
    snprintf(buf, 20, "s_%d", c);
    String s(buf);
    assert(df->get_string(0, ii)->equals(&s));
  }

  delete df;

  df = kd.get(&summer);

  assert(df->ncols() == 2);
  assert(df->nrows() == count);
  for (size_t ii = 0; ii < count; ii++) {
    int c = df->get_int(1, ii);
    snprintf(buf, 20, "s_%d", c);
    String s(buf);
    assert(df->get_string(0, ii)->equals(&s));
  }

  delete df;

  printf("Dataframe from rower summer test passed!\n");
}

void test_from_rower_file() {
  FileReader fr("data/words.txt");
  Key key("key", 0);
  KD_Store kd(0);
  DataFrame* df = DataFrame::from_rower(&key, &kd, "S", fr);

  String hi("hi");
  String hello("hello");
  String yo("yo");

  assert(df->ncols() == 1);
  assert(df->nrows() == 6);
  assert(df->get_string(0, 0)->equals(&hi));
  assert(df->get_string(0, 1)->equals(&hello));
  assert(df->get_string(0, 2)->equals(&yo));
  assert(df->get_string(0, 3)->equals(&yo));
  assert(df->get_string(0, 4)->equals(&hi));
  assert(df->get_string(0, 5)->equals(&hi));

  delete df;

  df = kd.get(&key);

  assert(df->ncols() == 1);
  assert(df->nrows() == 6);
  assert(df->get_string(0, 0)->equals(&hi));
  assert(df->get_string(0, 1)->equals(&hello));
  assert(df->get_string(0, 2)->equals(&yo));
  assert(df->get_string(0, 3)->equals(&yo));
  assert(df->get_string(0, 4)->equals(&hi));
  assert(df->get_string(0, 5)->equals(&hi));

  delete df;

  printf("Dataframe from rower file reader test passed!\n");
}

void test_local_map() {
  int cpid[2];
  String* server_ip = new String("127.0.0.1");
  String* client_ip1 = new String("127.0.0.2");
  String* client_ip2 = new String("127.0.0.3");

  // Fork to create another process
  if ((cpid[0] = fork())) {
      
  } else {
    // In child process
    sleep(0.5);
    KD_Store* kd = new KD_Store(1, client_ip1->c_str(), server_ip->c_str());
    sleep(2);

    Key* key = new Key("key", 0);
    String* hi = new String("hi");
    DataFrame* df = kd->wait_and_get(key);
    
    SIMap* map = new SIMap();
    Adder* adder = new Adder(*map);
    df->local_map(*adder);

    assert(map->count_ == 1);
    assert(map->get(hi)->value == 200);

    kd->application_complete();

    delete kd;
    delete key;
    delete hi;
    delete df;
    delete map;
    delete adder;
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
    sleep(0.5);
    KD_Store* kd = new KD_Store(0, client_ip2->c_str(), server_ip->c_str());
    sleep(1);

    size_t count = 300;
    Key* key = new Key("key", 0);
    String* hi = new String("hi");
    String** vals = new String*[count];
    for (int ii = 0; ii < count; ii++) vals[ii] = hi;
    DataFrame* df = DataFrame::from_array(key, kd, count, vals);
    
    SIMap* map = new SIMap();
    Adder* adder = new Adder(*map);
    df->local_map(*adder);

    assert(map->count_ == 1);
    assert(map->get(hi)->value == 100);

    kd->application_complete();

    delete kd;
    delete key;
    delete[] vals;
    delete df;
    delete map;
    delete adder;
    delete hi;
    delete server_ip;
    delete client_ip1;
    delete client_ip2;

    // exit
    exit(0);
  }

  // In parent process

  // Start server
  RServer* server = new RServer(server_ip->c_str()); 
  server->run_server(10);
  server->wait_for_shutdown();

  // wait for child to finish
  int st;
  waitpid(cpid[0], &st, 0);
  waitpid(cpid[1], &st, 0);
  delete server;
  delete client_ip1;
  delete client_ip2;
  delete server_ip;
  printf("Dataframe local map test passed!\n");
}

int main(int argc, char **argv) {
  max_test();
  min_test();

  // Column tests
  push_back_tests();
  get_type_tests(); 
  size_tests();
  int_column_set_tests();
  double_column_set_tests(); 
  bool_column_set_tests();
  string_column_set_tests();

  // Schema Tests
  schema_constructor_tests();
  schema_add_column_tests();
  schema_add_row_tests();

  // Row tests
  test_row_width();
  test_col_type();
  test_set_get();

  // Dataframe tests
  test_get_schema();
  test_add_column();
  dataframe_constructor_tests();
  dataframe_getters_tests();
  dataframe_fill_row_tests();
  dataframe_add_row_tests();
  test();

  // Map
  test_pmap_add();
  test_map_add();
  test_local_map();

  // From Constructors
  test_from_array_int();
  test_from_array_double();
  test_from_array_bool();
  test_from_array_string();
  test_from_file();
  test_from_scalar();
  test_from_rower_summer();
  test_from_rower_file();

  printf("All dataframe tests pass!\n");
}
