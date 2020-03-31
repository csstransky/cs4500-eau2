#include <gtest/gtest.h>
#include "../../src/dataframe/dataframe.h" 
#include "../../src/kv_store/kd_store.h"

#define GT_TRUE(a)   ASSERT_EQ((a),true)
#define GT_FALSE(a)  ASSERT_EQ((a),false)
#define GT_EQUALS(a, b)   ASSERT_EQ(a, b)
#define ASSERT_EXIT_ZERO(a)  \
  ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*")

const int NUM_ROWS = 1000 * 1000;

/*****************************************************************************
 * SumBytesFielder::
 * This fielder sums the size in bytes of each field.
 */
class SumBytesFielder : public Fielder {
public:
  size_t* sum_;

  SumBytesFielder(size_t* sum) {
    sum_ = sum;
  }

  ~SumBytesFielder() {

  }

  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r)  {

  }
 
  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b) {
    *sum_ += sizeof(bool);
  }

  virtual void accept(double f) {
    *sum_ += sizeof(double);
  }

  virtual void accept(int i) {
    *sum_ += sizeof(int);
  }

  virtual void accept(String* s) {
    *sum_ += sizeof(String*);
  }
 
  /** Called when all fields have been seen. */
  virtual void done() {

  }
  
};

/*******************************************************************************
 *  NonEmptyFilterRower::
 *  A Rower to determine rows that consist of not all empty fields.
 */
class NonEmptyFilterRower : public Rower {
 public:

  NonEmptyFilterRower() {

  }

  ~NonEmptyFilterRower() {

  }

  /** Return a copy of the object; nullptr is considered an error */
  Rower* clone() {
    return new NonEmptyFilterRower();
  }

  /** Returns false if the row has all empty fields. */
  bool accept(Row& r) {

    for (size_t i = 0; i < r.width(); i++) {
      switch (r.col_type(i))
      {
      case 'I': {
        int val = r.get_int(i);
        if (val != DEFAULT_INT_VALUE) {
          return true;
        }
        break;
      }
      case 'D': {
        double val = r.get_double(i);
        if (val != DEFAULT_DOUBLE_VALUE) {
          return true;
        }
        break;
      }
      case 'S': {
        String* val = r.get_string(i);
        if(!val->equals(&DEFAULT_STRING_VALUE)) {
          return true;
        }
        break;
      }
      case 'B': {
        bool val = r.get_bool(i);
        if (val != DEFAULT_BOOL_VALUE) {
          return true;
        }
        break;
      }
      default:
        // Should never reach this
        assert(0);
      }
    }

    return false;
  }

  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  void join_delete(Rower* other) {
    delete other;
  }
 
};

/*******************************************************************************
 *  AddRower::
 *  A Rower to add a constant to every int.
 */
class AddRower : public Rower {
 public:
  DataFrame* df_;

  AddRower(DataFrame* df) {
    df_ = df;
  }

  ~AddRower() {
    
  }

    /** Return a copy of the object; nullptr is considered an error */
  Rower* clone() {
    return new AddRower(df_);
  }

  /** Always returns true. Adds the num to each integer. */
  bool accept(Row& r) {
    int sum_ = 0;
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
    df_->set((size_t)r.width() - 1, r.get_idx(), sum_);    

    return true;
  }

  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  void join_delete(Rower* other) {
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
  exit(0);
}

void max_test() {
    size_t large = 100;
    size_t small = 20;
    size_t maximum = max_(large, small);
    GT_EQUALS(large, maximum);
    size_t maximum2 = max_(small, large);
    GT_EQUALS(large, maximum2);
    GT_TRUE(maximum2 != small);
    exit(0);
}

void min_test() {
    size_t large = 1000;
    size_t small = 1;
    size_t minimum = min_(large, small);
    GT_EQUALS(small, minimum);
    size_t minimum2 = min_(small, large);
    GT_EQUALS(small, minimum2);
    GT_TRUE(minimum2 != large);
    exit(0);
}

void converter_tests() {
    Column* int_column = new IntColumn();
    Column* double_column = new DoubleColumn();
    Column* bool_column = new BoolColumn();
    Column* string_column = new StringColumn();

    GT_TRUE(int_column->as_int() != nullptr);
    GT_TRUE(int_column->as_double() == nullptr);
    GT_TRUE(int_column->as_bool() == nullptr);
    GT_TRUE(int_column->as_string() == nullptr);

    GT_TRUE(double_column->as_int() == nullptr);
    GT_TRUE(double_column->as_double() != nullptr);
    GT_TRUE(double_column->as_bool() == nullptr);
    GT_TRUE(double_column->as_string() == nullptr);


    GT_TRUE(bool_column->as_int() == nullptr);
    GT_TRUE(bool_column->as_double() == nullptr);
    GT_TRUE(bool_column->as_bool() != nullptr);
    GT_TRUE(bool_column->as_string() == nullptr);

    GT_TRUE(string_column->as_int() == nullptr);
    GT_TRUE(string_column->as_double() == nullptr);
    GT_TRUE(string_column->as_bool() == nullptr);
    GT_TRUE(string_column->as_string() != nullptr);

    delete int_column;
    delete double_column;
    delete bool_column;
    delete string_column;
    exit(0);
}

void push_back_tests() {
    Column* int_column = new IntColumn();
    Column* double_column = new DoubleColumn();
    Column* bool_column = new BoolColumn();
    Column* string_column = new StringColumn();

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
    exit(0);
}

void get_type_tests() {
    Column* int_column = new IntColumn();
    Column* double_column = new DoubleColumn();
    Column* bool_column = new BoolColumn();
    Column* string_column = new StringColumn();

    GT_EQUALS(int_column->get_type(), 'I');
    GT_EQUALS(double_column->get_type(), 'D');
    GT_EQUALS(bool_column->get_type(), 'B');
    GT_EQUALS(string_column->get_type(), 'S');

    delete int_column;
    delete double_column;
    delete bool_column;
    delete string_column;  
    exit(0);  
}

void size_tests() {
    KV_Store kv(0);
    String c("c");
  
    Column* int_column = new IntColumn(&kv, &c, 0);
    Column* double_column = new DoubleColumn(&kv, &c, 0);
    Column* bool_column = new BoolColumn(&kv, &c, 0);
    Column* string_column = new StringColumn(&kv, &c, 0);

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
    exit(0);
}

void int_column_set_tests() {
    IntColumn* int_column = new IntColumn();
    int_column->push_back(23);
    int_column->push_back(42);
    int_column->push_back(5);
    int_column->push_back(5234);
    int_column->push_back(2342);
    int_column->push_back(2);
    int_column->set(0, 1);
    int_column->set(4, 2);
    int_column->set(5, 1);
    int_column->push_back(188);
    GT_EQUALS(int_column->size(), 7);
    GT_EQUALS(int_column->get(0), 1);
    GT_EQUALS(int_column->get(3), 5234);
    GT_EQUALS(int_column->get(4), 2);
    GT_EQUALS(int_column->get(5), 1);
    GT_EQUALS(int_column->get(6), 188);

    delete int_column;
    exit(0);
}

void double_column_set_tests() {
    DoubleColumn* double_column = new DoubleColumn();
    double_column->push_back(23.12);
    double_column->push_back(42.3);
    double_column->push_back(5.1);
    double_column->push_back(5234);
    double_column->push_back(2342.33);
    double_column->push_back(2.1);

    double_column->set(0, 1.11);
    double_column->set(4, 2.2);
    double_column->set(5, 1.33);
    double_column->push_back(188);
    GT_EQUALS(double_column->size(), 7);
    GT_EQUALS(double_column->get(0), (double)1.11);
    GT_EQUALS(double_column->get(3), (double)5234);
    GT_EQUALS(double_column->get(4), (double)2.2);
    GT_EQUALS(double_column->get(5), (double)1.33);
    GT_EQUALS(double_column->get(6), (double)188);

    delete double_column;
    exit(0);
}

void bool_column_set_tests() {
    BoolColumn* bool_column = new BoolColumn();
    bool_column->push_back(0);
    bool_column->push_back(false);
    bool_column->push_back(1);
    bool_column->push_back(true);
    bool_column->push_back(true);
    bool_column->push_back(false);

    bool_column->set(0, 1);
    bool_column->set(4, 2);
    bool_column->set(5, true);
    bool_column->push_back(false);
    GT_EQUALS(bool_column->size(), 7);
    GT_EQUALS(bool_column->get(0), 1);
    GT_EQUALS(bool_column->get(3), true);
    GT_EQUALS(bool_column->get(4), 1);
    GT_EQUALS(bool_column->get(5), 1);
    GT_EQUALS(bool_column->get(6), false);

    delete bool_column;
    exit(0);
}

void string_column_set_tests() {
    String* string1 = new String("lol");
    String* string2 = new String("BIG LOL");
    String* string3 = new String("Someone end this alreayd");
    StringColumn* string_column = new StringColumn();
    string_column->push_back(string1);
    string_column->push_back(string2);
    string_column->push_back(string3);
    string_column->set(0, string2);
    string_column->set(2, string1);
    string_column->push_back(string3);
    GT_EQUALS(string_column->size(), 4);
    GT_TRUE(string_column->get(0)->equals(string2));
    GT_TRUE(string_column->get(1)->equals(string2));
    GT_TRUE(string_column->get(2)->equals(string1));
    GT_TRUE(string_column->get(3)->equals(string3));

    delete string1;
    delete string2;
    delete string3;
    delete string_column;
    exit(0);
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
    exit(0);
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
    exit(0);
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
    exit(0);
}

void test_row_idx() {
  Schema* s = new Schema("II");
  Row* row = new Row(*s);
  GT_EQUALS(row->get_idx(), SIZE_MAX);
  row->set_idx(2);
  GT_EQUALS(row->get_idx(), 2);

  delete row;
  delete s;
  exit(0);
}

void test_row_width() {
  Schema* s = new Schema("II");
  Row* row = new Row(*s);
  GT_EQUALS(row->width(), 2);

  delete row;
  delete s;
  exit(0);
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
  exit(0);
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
  exit(0);
}

void test_sum_bytes() {
  Schema* s = new Schema("IDSB");
  Row* row = new Row(*s);
  String* hi = new String("hi");
  size_t sum_actual = 0;
  SumBytesFielder* f = new SumBytesFielder(&sum_actual);
  size_t sum_expect = sizeof(int) + sizeof(double) + sizeof(bool) + sizeof(String*);

  row->set(0, 4);
  row->set(1, (double)3.2);
  row->set(2, hi);
  row->set(3, (bool)0);
  row->visit(0, *f);

  GT_EQUALS(sum_actual, sum_expect);

  delete row;
  delete hi;
  delete f;
  delete s;
  exit(0);
}

void test_nonempty_filter_rower() {
  Schema* s = new Schema("IDSB");
  Rower* rower = new NonEmptyFilterRower();
  Row* row = new Row(*s);
  String* hi = new String("hi");
  String* empty = new String("");

  row->set(0, 4);
  row->set(1, (double)3.2);
  row->set(2, hi);
  row->set(3, (bool)0);
  
  GT_EQUALS(rower->accept(*row), 1);

  row->set(0, (int)0);
  row->set(1, (double)0);
  row->set(2, empty);
  row->set(3, (bool)0);

  GT_EQUALS(rower->accept(*row), 0);

  delete rower;
  delete row;
  delete hi;
  delete empty;
  exit(0);
}

void test_map() {
  KV_Store kv(0);
  String c("c");
  Schema* s = new Schema("IIDI");
  DataFrame* df = new DataFrame(*s, &c, &kv);
  Row* r = new Row(df->get_schema());
  AddRower* rower = new AddRower(df);

  for (size_t i = 0; i <  10; i++) {
    r->set(0,(int)i);
    r->set(1,(int)i+1);
    r->set(2, (double)i);
    df->add_row(*r);
  }

  df->map(*rower);

  for (size_t i = 0; i < 10; i++) {
    GT_EQUALS(df->get_int(3,i), i+i+1);
  }

  for (size_t i = 0; i < 10; i++) {
    GT_EQUALS(df->get_int((size_t)0,i), i);
    GT_EQUALS(df->get_int((size_t)1,i), i+1);
    GT_EQUALS(df->get_double((size_t)2,i), (double)i);
  }

  delete s;
  delete df;
  delete r;
  delete rower;
  
  exit(0);
}

void test_filter() {
  KV_Store kv(0);
  String c("c");
  Schema* s = new Schema("IDSB");
  DataFrame* df = new DataFrame(*s, &c, &kv);
  
  Rower* rower = new NonEmptyFilterRower();
  Row* row = new Row(*s);
  String* hi = new String("hi");
  String* empty = nullptr;

  // Non empty row
  row->set(0, 4);
  row->set(1, (double)3.2);
  row->set(2, hi);
  row->set(3, (bool)0);
  df->add_row(*row);

  // Empty row
  row->set(0, (int)0);
  row->set(1, (double)0);
  row->set(2, empty);
  row->set(3, (bool)0);
  df->add_row(*row);

  // Empty row
  row->set(0, (int)0);
  row->set(1, (double)0);
  row->set(2, empty);
  row->set(3, (bool)0);
  df->add_row(*row);

  // Non empty row
  row->set(0, 4);
  row->set(1, (double)3.2);
  row->set(2, hi);
  row->set(3, (bool)0);
  df->add_row(*row);

  GT_EQUALS(df->nrows(), 4);
  String filter("filtered");
  DataFrame* df_new = df->filter(*rower, &filter);

  GT_EQUALS(df_new->nrows(), 2);

  for (size_t i = 0; i < 2; i++) {
    GT_EQUALS(df_new->get_int((size_t)0,i), 4);
    GT_EQUALS(df_new->get_double((size_t)1,i), (double)3.2);
    GT_EQUALS(df_new->get_string((size_t)2,i)->equals(hi), 1);
    GT_EQUALS(df_new->get_bool((size_t)3,i), (bool)0);
  }

  delete rower;
  delete row;
  delete hi;
  delete empty;
  delete df_new;
  delete df;
  delete s;
  exit(0);
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

  exit(0);
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

  Column* c_int = new IntColumn();
  c_int->push_back(1);
  c_int->push_back(2);
  c_int->push_back(3);
  c_int->push_back(4);
  Column* c_double = new DoubleColumn();
  c_double->push_back((double)1.2);
  c_double->push_back((double)2.2);
  c_double->push_back((double)3.2);
  c_double->push_back((double)4.2);
  Column* c_string = new StringColumn();
  c_string->push_back(hi);
  c_string->push_back(hi);
  c_string->push_back(hi);
  c_string->push_back(hi);
  Column* c_bool = new BoolColumn();
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

  exit(0);
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
  IntColumn* int_column = new IntColumn();
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
  exit(0);
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

  Column* c_int = new IntColumn();
  c_int->push_back(1);
  c_int->push_back(3);
  c_int->push_back(4);
  c_int->push_back(2);
  Column* c_double = new DoubleColumn();
  c_double->push_back((double)1.2);
  c_double->push_back((double)3.2);
  c_double->push_back((double)2);
  c_double->push_back((double)1);
  Column* c_string = new StringColumn();
  c_string->push_back(hi);
  c_string->push_back(hello);
  c_string->push_back(nullptr);
  c_string->push_back(hi);
  c_string->push_back(h);
  Column* c_bool = new BoolColumn();
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

  exit(0);
}

void dataframe_setters_tests() { 
  KV_Store kv(0);
  String c("c");
  Schema s1("");
  DataFrame df(s1, &c, &kv);
  String* hi = new String("hi");
  String* hello = new String("hello");
  String* h = new String("h");

  GT_EQUALS(df.ncols(), 0);
  GT_EQUALS(df.nrows(), 0);

  Column* c_int = new IntColumn();
  c_int->push_back(1);
  c_int->push_back(3);
  c_int->push_back(4);
  c_int->push_back(2);
  Column* c_double = new DoubleColumn();
  c_double->push_back((double)1.2);
  c_double->push_back((double)3.2);
  c_double->push_back((double)2);
  c_double->push_back((double)1);
  Column* c_string = new StringColumn();
  c_string->push_back(hi);
  c_string->push_back(hello);
  c_string->push_back(nullptr);
  c_string->push_back(hi);
  c_string->push_back(h);
  Column* c_bool = new BoolColumn();
  c_bool->push_back((bool)0);
  c_bool->push_back((bool)1);
  c_bool->push_back((bool)1);

  df.add_column(c_int);
  df.add_column(c_double);
  df.add_column(c_string);
  df.add_column(c_bool);

  for (size_t ii = 0; ii < df.nrows(); ii++) {
    df.set(0, ii, 14);
    df.set(1, ii, (double)12.44);
    df.set(2, ii, hello);
    df.set(3, ii, true);
  }

  for (size_t ii = 0; ii < df.nrows(); ii++) {
    GT_EQUALS(df.get_int(0, ii), 14);
    GT_EQUALS(df.get_double(1, ii), (double)12.44);
    GT_TRUE(df.get_string(2, ii)->equals(hello));
    GT_EQUALS(df.get_bool(3, ii), true);
  }

  delete h;
  delete hi;
  delete hello;
  delete c_int;
  delete c_bool;
  delete c_double;
  delete c_string;

  exit(0);
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

  Column* c_int = new IntColumn();
  c_int->push_back(1);
  c_int->push_back(3);
  c_int->push_back(4);
  c_int->push_back(2);
  Column* c_double = new DoubleColumn();
  c_double->push_back((double)1.2);
  c_double->push_back((double)3.2);
  c_double->push_back((double)2);
  c_double->push_back((double)1);
  Column* c_string = new StringColumn();
  c_string->push_back(hi);
  c_string->push_back(hello);
  c_string->push_back(nullptr);
  c_string->push_back(hi);
  c_string->push_back(h);
  Column* c_bool = new BoolColumn();
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

  exit(0);
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

  Column* c_int = new IntColumn();
  c_int->push_back(1);
  c_int->push_back(3);
  c_int->push_back(4);
  c_int->push_back(2);
  Column* c_double = new DoubleColumn();
  c_double->push_back((double)1.2);
  c_double->push_back((double)3.2);
  c_double->push_back((double)2);
  c_double->push_back((double)1);
  Column* c_string = new StringColumn();
  c_string->push_back(hi);
  c_string->push_back(hello);
  c_string->push_back(nullptr);
  c_string->push_back(hi);
  c_string->push_back(h);
  Column* c_bool = new BoolColumn();
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

  exit(0);
}

void test_pmap_add() {
  KV_Store kv(0);
  String c("c");
  Schema s("IIDI");
  DataFrame df(s, &c, &kv);
  Row  r(df.get_schema());
  AddRower rower(&df);

  for (size_t i = 0; i < NUM_ROWS; i++) {
    r.set(0,(int)i);
    r.set(1,(int)i+1);
    r.set(2, (double)i);
    df.add_row(r);
  }

  df.pmap(rower);

  for (size_t i = 0; i < NUM_ROWS; i++) {
    GT_EQUALS(df.get_int(3,i), i+i+1);
  }

  for (size_t i = 0; i < NUM_ROWS; i++) {
    GT_EQUALS(df.get_int((size_t)0,i), i);
    GT_EQUALS(df.get_int((size_t)1,i), i+1);
    GT_EQUALS(df.get_double((size_t)2,i), (double)i);
  }
  
  exit(0);
}

void test_map_add() {
  KV_Store kv(0);
  String c("c");
  Schema s("IIDI");
  DataFrame df(s, &c, &kv);
  Row  r(df.get_schema());
  AddRower rower(&df);

  for (size_t i = 0; i <  NUM_ROWS; i++) {
    r.set(0,(int)i);
    r.set(1,(int)i+1);
    r.set(2, (double)i);
    df.add_row(r);
  }

  df.map(rower);

  for (size_t i = 0; i < NUM_ROWS; i++) {
    GT_EQUALS(df.get_int(3,i), i+i+1);
  }

  for (size_t i = 0; i < NUM_ROWS; i++) {
    GT_EQUALS(df.get_int((size_t)0,i), i);
    GT_EQUALS(df.get_int((size_t)1,i), i+1);
    GT_EQUALS(df.get_double((size_t)2,i), (double)i);
  }
  
  exit(0);
}

TEST(a4, t1){ ASSERT_EXIT_ZERO(test); }

// Helper function tests
TEST(helper_function, max_test){ ASSERT_EXIT_ZERO(max_test); }
TEST(helper_function, min_test){ ASSERT_EXIT_ZERO(min_test); }

// Column Tests
TEST(Column, converter_tests){ ASSERT_EXIT_ZERO(converter_tests); }
TEST(Column, push_back_tests){ ASSERT_EXIT_ZERO(push_back_tests); }
TEST(Column, get_type_tests){ ASSERT_EXIT_ZERO(get_type_tests); }
TEST(Column, size_tests){ ASSERT_EXIT_ZERO(size_tests); }
TEST(IntColumn, int_column_set_tests){ ASSERT_EXIT_ZERO(int_column_set_tests); }
TEST(DoubleColumn, double_column_set_tests){ ASSERT_EXIT_ZERO(double_column_set_tests); }
TEST(BoolColumn, bool_column_set_tests){ ASSERT_EXIT_ZERO(bool_column_set_tests); }
TEST(StringColumn, string_column_set_tests){ ASSERT_EXIT_ZERO(string_column_set_tests); }

// Schema Tests
TEST(Schema, schema_constructor_tests){ ASSERT_EXIT_ZERO(schema_constructor_tests); }
TEST(Schema, schema_add_column_tests){ ASSERT_EXIT_ZERO(schema_add_column_tests); }
TEST(Schema, schema_add_row_tests){ ASSERT_EXIT_ZERO(schema_add_row_tests); }

// Row tests
TEST(Row, test_row_idx){ ASSERT_EXIT_ZERO(test_row_idx); }
TEST(Row, test_row_width){ ASSERT_EXIT_ZERO(test_row_width); }
TEST(Row, test_col_type){ ASSERT_EXIT_ZERO(test_col_type);}
TEST(Row, test_set_get){ ASSERT_EXIT_ZERO(test_set_get);}

// Fielder tests
TEST(Fielder, test_sum_bytes){ ASSERT_EXIT_ZERO(test_sum_bytes);}

// Rower tests
TEST(Rower, test_nonempty_filter_rower){ ASSERT_EXIT_ZERO(test_nonempty_filter_rower);}

// Dataframe tests
TEST(DataFrame, test_map){ ASSERT_EXIT_ZERO(test_map);} 
TEST(DataFrame, test_filter){ ASSERT_EXIT_ZERO(test_filter);}
TEST(DataFrame, test_get_schema){ ASSERT_EXIT_ZERO(test_get_schema);}
TEST(DataFrame, test_add_column){ ASSERT_EXIT_ZERO(test_add_column);}
TEST(DataFrame, dataframe_constructor_tests){ ASSERT_EXIT_ZERO(dataframe_constructor_tests); }
TEST(DataFrame, dataframe_getters_tests){ ASSERT_EXIT_ZERO(dataframe_getters_tests); }
TEST(DataFrame, dataframe_setters_tests){ ASSERT_EXIT_ZERO(dataframe_setters_tests); }
TEST(DataFrame, dataframe_fill_row_tests){ ASSERT_EXIT_ZERO(dataframe_fill_row_tests); }
TEST(DataFrame, dataframe_add_row_tests){ ASSERT_EXIT_ZERO(dataframe_add_row_tests); }

// Parallel map
TEST(DataFrame, test_pmap_add){ ASSERT_EXIT_ZERO(test_pmap_add);}
TEST(DataFrame, test_map_add){ ASSERT_EXIT_ZERO(test_map_add);}

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

  char* schema_types = dataframe->get_schema().types_;
  assert(strcmp("SISSBBBB", schema_types) == 0);
    
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
  assert(strcmp("SISSBBBB", schema_types) == 0);
    
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

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  int val = 0;;
  val = RUN_ALL_TESTS();
  test_from_array_int();
  test_from_array_double();
  test_from_array_bool();
  test_from_array_string();
  test_from_file();
  return val;
}
