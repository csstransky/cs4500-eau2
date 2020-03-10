#include <gtest/gtest.h>
#include "modified_dataframe.h" 

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

  virtual void accept(float f) {
    *sum_ += sizeof(float);
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
  Object* clone() {
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
      case 'F': {
        float val = r.get_float(i);
        if (val != DEFAULT_FLOAT_VALUE) {
          return true;
        }
        break;
      }
      case 'S': {
        String* val = r.get_string(i);
        if(val != DEFAULT_STRING_VALUE) {
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
  Object* clone() {
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

  DataFrame df(s);
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
    Column* float_column = new FloatColumn();
    Column* bool_column = new BoolColumn();
    Column* string_column = new StringColumn();

    GT_TRUE(int_column->as_int() != nullptr);
    GT_TRUE(int_column->as_float() == nullptr);
    GT_TRUE(int_column->as_bool() == nullptr);
    GT_TRUE(int_column->as_string() == nullptr);

    GT_TRUE(float_column->as_int() == nullptr);
    GT_TRUE(float_column->as_float() != nullptr);
    GT_TRUE(float_column->as_bool() == nullptr);
    GT_TRUE(float_column->as_string() == nullptr);


    GT_TRUE(bool_column->as_int() == nullptr);
    GT_TRUE(bool_column->as_float() == nullptr);
    GT_TRUE(bool_column->as_bool() != nullptr);
    GT_TRUE(bool_column->as_string() == nullptr);

    GT_TRUE(string_column->as_int() == nullptr);
    GT_TRUE(string_column->as_float() == nullptr);
    GT_TRUE(string_column->as_bool() == nullptr);
    GT_TRUE(string_column->as_string() != nullptr);

    delete int_column;
    delete float_column;
    delete bool_column;
    delete string_column;
    exit(0);
}

void push_back_tests() {
    Column* int_column = new IntColumn();
    Column* float_column = new FloatColumn();
    Column* bool_column = new BoolColumn();
    Column* string_column = new StringColumn();

    int int_value = 2;
    float float_value = 2.2;
    bool bool_value = true;
    String* string_value = new String("lol");

    int_column->push_back(int_value);
    GT_EQUALS(int_column->size(), 1);
    float_column->push_back(float_value);
    GT_EQUALS(float_column->size(), 1);
    bool_column->push_back(bool_value);
    GT_EQUALS(bool_column->size(), 1);
    string_column->push_back(string_value);
    GT_EQUALS(string_column->size(), 1);

    delete int_column;
    delete float_column;
    delete bool_column;
    delete string_column;  
    delete string_value; 
    exit(0);
}

void get_type_tests() {
    Column* int_column = new IntColumn();
    Column* float_column = new FloatColumn();
    Column* bool_column = new BoolColumn();
    Column* string_column = new StringColumn();

    GT_EQUALS(int_column->get_type(), 'I');
    GT_EQUALS(float_column->get_type(), 'F');
    GT_EQUALS(bool_column->get_type(), 'B');
    GT_EQUALS(string_column->get_type(), 'S');

    delete int_column;
    delete float_column;
    delete bool_column;
    delete string_column;  
    exit(0);  
}

void size_tests() {
    Column* int_column = new IntColumn();
    Column* float_column = new FloatColumn();
    Column* bool_column = new BoolColumn();
    Column* string_column = new StringColumn();

    int int_value = 2;
    float float_value = 2.2;
    bool bool_value = true;
    String* string_value = new String("lol");

    for (size_t ii = 0; ii < 100; ii++) {
        int_column->push_back(int_value);
        GT_EQUALS(int_column->size(), ii + 1);
    }
    for (size_t ii = 0; ii < 100; ii++) {
        float_column->push_back(float_value);
        GT_EQUALS(float_column->size(), ii + 1);
    }
    for (size_t ii = 0; ii < 100; ii++) {
        bool_column->push_back(bool_value);
        GT_EQUALS(bool_column->size(), ii + 1);
    }
    for (size_t ii = 0; ii < 100; ii++) {
        string_column->push_back(string_value);
        GT_EQUALS(string_column->size(), ii + 1);
    }

    delete int_column;
    delete float_column;
    delete bool_column;
    delete string_column;  
    delete string_value; 
    exit(0);
}

void int_column_constructor_tests() {
    IntColumn* int_column = new IntColumn(6, 23, 42, 5, 5234, 2342, 2);
    GT_EQUALS(int_column->size(), 6);
    GT_EQUALS(int_column->get(0), 23);
    GT_EQUALS(int_column->get(3), 5234);
    GT_EQUALS(int_column->get(5), 2);

    IntColumn* int_column2 = new IntColumn(100, 3, 4);
    GT_EQUALS(int_column2->size(), 100);
    GT_EQUALS(int_column2->get(1), 4);
    GT_EQUALS(int_column2->get(99), 0);

    delete int_column;
    delete int_column2;
    exit(0);
}

void int_column_set_tests() {
    IntColumn* int_column = new IntColumn(6, 23, 42, 5, 5234, 2342, 2);
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

    IntColumn* int_column2 = new IntColumn(100, 3, 4);
    int_column2->set(0, 14);
    int_column2->set(36, 144);
    int_column2->set(99, 1);
    int_column2->push_back(133);
    GT_EQUALS(int_column2->size(), 101);
    GT_EQUALS(int_column2->get(0), 14);
    GT_EQUALS(int_column2->get(1), 4);
    GT_EQUALS(int_column2->get(36), 144);
    GT_EQUALS(int_column2->get(99), 1);
    GT_EQUALS(int_column2->get(100), 133);

    delete int_column;
    delete int_column2;
    exit(0);
}

void float_column_constructor_tests() {
    FloatColumn* float_column = new FloatColumn(6, 23.23, 42.33, 5.0, 5234.1123, (float)2342, 2.1);
    GT_EQUALS(float_column->size(), 6);
    GT_EQUALS(float_column->get(0), (float)23.23);
    GT_EQUALS(float_column->get(3), (float)5234.1123);
    GT_EQUALS(float_column->get(5), (float)2.1);

    FloatColumn* float_column2 = new FloatColumn(100, 3.1, 4.44);
    GT_EQUALS(float_column2->size(), 100);
    GT_EQUALS(float_column2->get(1), (float)4.44);
    GT_EQUALS(float_column2->get(99), (float)0);

    delete float_column;
    delete float_column2;
    exit(0);
}

void float_column_set_tests() {
    FloatColumn* float_column = new FloatColumn(6, 23.12, 42.3, 5.1, (float)5234, 2342.33, 2.1);
    float_column->set(0, 1.11);
    float_column->set(4, 2.2);
    float_column->set(5, 1.33);
    float_column->push_back(188);
    GT_EQUALS(float_column->size(), 7);
    GT_EQUALS(float_column->get(0), (float)1.11);
    GT_EQUALS(float_column->get(3), (float)5234);
    GT_EQUALS(float_column->get(4), (float)2.2);
    GT_EQUALS(float_column->get(5), (float)1.33);
    GT_EQUALS(float_column->get(6), (float)188);

    FloatColumn* float_column2 = new FloatColumn(100, 3.1, 4.2);
    float_column2->set(0, 14.11);
    float_column2->set(36, 144.2);
    float_column2->set(99, 1.33333);
    float_column2->push_back(133.1);
    GT_EQUALS(float_column2->size(), 101);
    GT_EQUALS(float_column2->get(0), (float)14.11);
    GT_EQUALS(float_column2->get(1), (float)4.2);
    GT_EQUALS(float_column2->get(36), (float)144.2);
    GT_EQUALS(float_column2->get(99), (float)1.33333);
    GT_EQUALS(float_column2->get(100), (float)133.1);

    delete float_column;
    delete float_column2;
    exit(0);
}

void bool_column_constructor_tests() {
    BoolColumn* bool_column = new BoolColumn(6, 0, false, 0, 1, 1, true);
    GT_EQUALS(bool_column->size(), 6);
    GT_EQUALS(bool_column->get(0), false);
    GT_EQUALS(bool_column->get(3), true);
    GT_EQUALS(bool_column->get(5), 1);

    BoolColumn* bool_column2 = new BoolColumn(100, false, true);
    GT_EQUALS(bool_column2->size(), 100);
    GT_EQUALS(bool_column2->get(1), true);
    GT_EQUALS(bool_column2->get(99), 0);

    delete bool_column;
    delete bool_column2;
    exit(0);
}

void bool_column_set_tests() {
    BoolColumn* bool_column = new BoolColumn(6, 0, false, 1, true, true, false);
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

    BoolColumn* bool_column2 = new BoolColumn(100, true, false);
    bool_column2->set(0, true);
    bool_column2->set(36, 0);
    bool_column2->set(99, 1);
    bool_column2->push_back(133);
    GT_EQUALS(bool_column2->size(), 101);
    GT_EQUALS(bool_column2->get(0), true);
    GT_EQUALS(bool_column2->get(1), 0);
    GT_EQUALS(bool_column2->get(36), false);
    GT_EQUALS(bool_column2->get(99), true);
    GT_EQUALS(bool_column2->get(100), true);

    delete bool_column;
    delete bool_column2;
    exit(0);
}

void string_column_constructor_tests() {
    String* string1 = new String("lol");
    String* string2 = new String("BIG LOL");
    String* string3 = new String("Someone end this alreayd");
    StringColumn* string_column = new StringColumn(3, string1, string2, string3);
    GT_EQUALS(string_column->size(), 3);
    GT_EQUALS(string_column->get(0), string1);
    GT_EQUALS(string_column->get(1), string2);
    GT_EQUALS(string_column->get(2), string3);

    StringColumn* string_column2 = new StringColumn(100, string1, string2);
    GT_EQUALS(string_column2->size(), 100);
    GT_EQUALS(string_column2->get(0), string1);
    GT_EQUALS(string_column2->get(1), string2);
    GT_EQUALS(string_column2->get(99), nullptr);

    delete string1;
    delete string2;
    delete string3;
    delete string_column;
    delete string_column2;
    exit(0);
}

void string_column_set_tests() {
    String* string1 = new String("lol");
    String* string2 = new String("BIG LOL");
    String* string3 = new String("Someone end this alreayd");
    StringColumn* string_column = new StringColumn(3, string1, string2, string3);
    string_column->set(0, string2);
    string_column->set(2, string1);
    string_column->push_back(string3);
    GT_EQUALS(string_column->size(), 4);
    GT_EQUALS(string_column->get(0), string2);
    GT_EQUALS(string_column->get(1), string2);
    GT_EQUALS(string_column->get(2), string1);
    GT_EQUALS(string_column->get(3), string3);

    StringColumn* string_column2 = new StringColumn(100, string1, string2);
    string_column2->set(0, string2);
    string_column2->set(27, string1);
    string_column2->set(99, string1);
    string_column2->push_back(string3);
    GT_EQUALS(string_column2->size(), 101);
    GT_EQUALS(string_column2->get(0), string2);
    GT_EQUALS(string_column2->get(1), string2);
    GT_EQUALS(string_column2->get(27), string1);
    GT_EQUALS(string_column2->get(99), string1);
    GT_EQUALS(string_column2->get(100), string3);

    delete string1;
    delete string2;
    delete string3;
    delete string_column;
    delete string_column2;
    exit(0);
}

void schema_constructor_tests() {
    Schema* schema1 = new Schema();
    GT_EQUALS(schema1->width(), 0);
    GT_EQUALS(schema1->length(), 0);
    GT_EQUALS(schema1->col_idx(""), -1);
    GT_EQUALS(schema1->row_idx(""), -1);

    const char* types = "ISBFFBS";
    Schema* schema2 = new Schema(types);
    GT_EQUALS(schema2->width(), 7);
    GT_EQUALS(schema2->length(), 0);
    GT_EQUALS(schema2->col_idx(""), -1);
    GT_EQUALS(schema2->row_idx(""), -1);
    for (size_t ii = 0; ii < schema2->width(); ii++) {
        GT_EQUALS(schema2->col_name(ii), nullptr);
        GT_EQUALS(schema2->col_type(ii), types[ii]);
    }
    
    Schema* schema3 = new Schema(*schema2);
    GT_EQUALS(schema3->width(), 7);
    GT_EQUALS(schema3->length(), 0);
    GT_EQUALS(schema3->col_idx(""), -1);
    GT_EQUALS(schema3->row_idx(""), -1);
    for (size_t ii = 0; ii < schema3->width(); ii++) {
        GT_EQUALS(schema3->col_name(ii), nullptr);
        GT_EQUALS(schema3->col_type(ii), types[ii]);
    }

    // Testing to see that the copied Schema is not linked to its original
    schema3->add_column('S', nullptr);
    schema3->add_row(nullptr);
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
        schema1->add_column('S', nullptr);
    }
    GT_EQUALS(schema1->width(), 100);
    GT_EQUALS(schema1->col_type(47), 'S');
    
    String* name = new String("hi");
    schema1->add_column('F', name);
    GT_EQUALS(schema1->width(), 101);
    GT_EQUALS(schema1->length(), 0);
    GT_EQUALS(schema1->col_type(100), 'F');
    GT_EQUALS(schema1->col_idx("hi"), 100);

    delete schema1;
    delete name;
    exit(0);
}

void schema_add_row_tests() {
    Schema* schema1 = new Schema();
    for (size_t ii = 0; ii < 100; ii++) {
        schema1->add_row(nullptr);
    }
    GT_EQUALS(schema1->length(), 100);
    
    String* name = new String("hi");
    schema1->add_row(name);
    GT_EQUALS(schema1->length(), 101);
    GT_EQUALS(schema1->width(), 0);
    GT_EQUALS(schema1->row_idx("hi"), 100);

    delete schema1;
    delete name;
    exit(0);
}

void schema_row_name_tests() {
    Schema* schema1 = new Schema();
    String* name1 = new String("lolp");
    String* name2 = new String("jojo");
    schema1->add_row(name1);
    schema1->add_row(name2);
    schema1->add_row(nullptr);
    GT_EQUALS(schema1->row_name(0), name1);
    GT_EQUALS(schema1->row_name(1), name2);
    GT_EQUALS(schema1->row_name(2), nullptr);
    delete name1;
    delete name2;
    delete schema1;
    exit(0);
}

void schema_col_name_tests() {
    Schema* schema1 = new Schema("I");
    String* name1 = new String("lolp");
    String* name2 = new String("jojo");
    schema1->add_column('F', name1);
    schema1->add_column('S', name2);
    schema1->add_column('B', nullptr);
    GT_EQUALS(schema1->col_name(0), nullptr);
    GT_EQUALS(schema1->col_name(1), name1);
    GT_EQUALS(schema1->col_name(2), name2);
    GT_EQUALS(schema1->col_name(3), nullptr);
    delete name1;
    delete name2;
    delete schema1;
    exit(0);
}

void schema_col_idx_tests() {
    Schema* schema1 = new Schema("FBSFB");
    String* name1 = new String("lolp");
    String* name2 = new String("jojo");
    schema1->add_column('F', nullptr);
    schema1->add_column('F', name1);
    schema1->add_column('S', name2);
    schema1->add_column('S', name2);
    schema1->add_column('B', name1);
    GT_EQUALS(schema1->col_idx("lolp"), 6);
    GT_EQUALS(schema1->col_idx("jojo"), 7);
    GT_EQUALS(schema1->col_idx("dodo"), -1);
    delete name1;
    delete name2;
    delete schema1;
    exit(0);
}

void schema_row_idx_tests() {
    Schema* schema1 = new Schema("FBSFB");
    String* name1 = new String("lolp");
    String* name2 = new String("jojo");
    schema1->add_row(nullptr);
    schema1->add_row(name1);
    schema1->add_row(name2);
    schema1->add_row(name2);
    schema1->add_row(name1);
    GT_EQUALS(schema1->row_idx("lolp"), 1);
    GT_EQUALS(schema1->row_idx("jojo"), 2);
    GT_EQUALS(schema1->row_idx("dodo"), -1);
    delete name1;
    delete name2;
    delete schema1;
    exit(0);
}

void test_row_idx() {
  Schema s("II");
  Row* row = new Row(s);
  GT_EQUALS(row->get_idx(), SIZE_MAX);
  row->set_idx(2);
  GT_EQUALS(row->get_idx(), 2);

  delete row;
  exit(0);
}

void test_row_width() {
  Schema s("II");
  Row* row = new Row(s);
  GT_EQUALS(row->width(), 2);

  delete row;
  exit(0);
}

void test_col_type() {
  Schema s("IFSB");
  Row* row = new Row(s);
  GT_EQUALS(row->col_type(0), 'I');
  GT_EQUALS(row->col_type(1), 'F');
  GT_EQUALS(row->col_type(2), 'S');
  GT_EQUALS(row->col_type(3), 'B');

  delete row;
  exit(0);
}

void test_set_get() {
  Schema* s = new Schema("IFSB");
  Row* row = new Row(*s);
  String* hi = new String("hi");
  row->set(0, 4);
  row->set(1, (float)3.2);
  row->set(2, hi);
  row->set(3, (bool)0);
  GT_EQUALS(row->get_int(0), 4);
  GT_EQUALS(row->get_float(1), (float)3.2);
  GT_EQUALS(row->get_string(2)->equals(hi), 1);
  GT_EQUALS(row->get_bool(3), 0);

  delete s;
  delete row;
  delete hi;
  exit(0);
}

void test_sum_bytes() {
  Schema s("IFSB");
  Row* row = new Row(s);
  String* hi = new String("hi");
  size_t sum_actual = 0;
  SumBytesFielder* f = new SumBytesFielder(&sum_actual);
  size_t sum_expect = sizeof(int) + sizeof(float) + sizeof(bool) + sizeof(String*);

  row->set(0, 4);
  row->set(1, (float)3.2);
  row->set(2, hi);
  row->set(3, (bool)0);
  row->visit(0, *f);

  GT_EQUALS(sum_actual, sum_expect);

  delete row;
  delete hi;
  delete f;
  exit(0);
}

void test_nonempty_filter_rower() {
  Schema s("IFSB");
  Rower* rower = new NonEmptyFilterRower();
  Row* row = new Row(s);
  String* hi = new String("hi");
  String* empty = nullptr;

  row->set(0, 4);
  row->set(1, (float)3.2);
  row->set(2, hi);
  row->set(3, (bool)0);
  
  GT_EQUALS(rower->accept(*row), 1);

  row->set(0, (int)0);
  row->set(1, (float)0);
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
  Schema s("IIFI");
  DataFrame df(s);
  Row  r(df.get_schema());
  AddRower rower(&df);

  for (size_t i = 0; i <  10; i++) {
    r.set(0,(int)i);
    r.set(1,(int)i+1);
    r.set(2, (float)i);
    df.add_row(r);
  }

  df.map(rower);

  for (size_t i = 0; i < 10; i++) {
    GT_EQUALS(df.get_int(3,i), i+i+1);
  }

  for (size_t i = 0; i < 10; i++) {
    GT_EQUALS(df.get_int((size_t)0,i), i);
    GT_EQUALS(df.get_int((size_t)1,i), i+1);
    GT_EQUALS(df.get_float((size_t)2,i), (float)i);
  }
  
  exit(0);
}

void test_filter() {
  Schema s("IFSB");
  DataFrame df(s);
  
  Rower* rower = new NonEmptyFilterRower();
  Row* row = new Row(s);
  String* hi = new String("hi");
  String* empty = nullptr;

  // Non empty row
  row->set(0, 4);
  row->set(1, (float)3.2);
  row->set(2, hi);
  row->set(3, (bool)0);
  df.add_row(*row);

  // Empty row
  row->set(0, (int)0);
  row->set(1, (float)0);
  row->set(2, empty);
  row->set(3, (bool)0);
  df.add_row(*row);

  // Empty row
  row->set(0, (int)0);
  row->set(1, (float)0);
  row->set(2, empty);
  row->set(3, (bool)0);
  df.add_row(*row);

  // Non empty row
  row->set(0, 4);
  row->set(1, (float)3.2);
  row->set(2, hi);
  row->set(3, (bool)0);
  df.add_row(*row);

  GT_EQUALS(df.nrows(), 4);

  DataFrame* df_new = df.filter(*rower);

  GT_EQUALS(df_new->nrows(), 2);

  for (size_t i = 0; i < 2; i++) {
    GT_EQUALS(df_new->get_int((size_t)0,i), 4);
    GT_EQUALS(df_new->get_float((size_t)1,i), (float)3.2);
    GT_EQUALS(df_new->get_string((size_t)2,i)->equals(hi), 1);
    GT_EQUALS(df_new->get_bool((size_t)3,i), (bool)0);
  }

  delete rower;
  delete row;
  delete hi;
  delete empty;
  delete df_new;
  exit(0);
}

void test_get_schema() {
  Schema s1("IFSB");
  DataFrame df(s1);

  Schema s2 = df.get_schema();

  for (int i = 0; i < 4; i++) {
    GT_EQUALS(s1.col_type(i), s2.col_type(i));
  }

  exit(0);
}

void test_add_column() {
  Schema s1("IFSB");
  DataFrame df(s1);
  String* hi = new String("hi");
  String* hello = new String("hello");
  String* h = new String("h");

  GT_EQUALS(df.ncols(), 4);
  GT_EQUALS(df.nrows(), 0);

  Column* c_int = new IntColumn(4, 1, 3, 4, 2);
  Column* c_float = new FloatColumn(4, (float)1.2, (float)3.2, (float)2, (float)1);
  Column* c_string = new StringColumn(4, hi, hi, hi, hi);
  Column* c_bool = new BoolColumn(4, (bool)0, (bool)1, (bool)1, (bool)1);

  df.add_column(c_int, hi);
  df.add_column(c_float, hello);
  df.add_column(c_string, hi);
  df.add_column(c_bool, nullptr);

  GT_EQUALS(df.get_schema().col_name(4)->equals(hi), 1);
  GT_EQUALS(df.get_schema().col_name(5)->equals(hello), 1);
  GT_EQUALS(df.get_schema().col_name(6)->equals(hi), 1);

  GT_EQUALS(df.get_col(*hi), 4);
  GT_EQUALS(df.get_col(*hello), 5);
  GT_EQUALS(df.get_col(*h), -1);

  GT_EQUALS(df.ncols(), 8);
  GT_EQUALS(df.nrows(), 4);

  for (int i = 0; i < 4; i++) {
    GT_EQUALS(df.get_int(0, i), DEFAULT_INT_VALUE);
    GT_EQUALS(df.get_float(1, i), DEFAULT_FLOAT_VALUE);
    GT_EQUALS(df.get_string(2, i), DEFAULT_STRING_VALUE);
    GT_EQUALS(df.get_bool(3, i), DEFAULT_BOOL_VALUE); 
  }
  
  for (int i = 0; i < 100; i++) {
    df.add_column(c_bool, nullptr);
  }

  GT_EQUALS(df.ncols(), 108);
  GT_EQUALS(df.nrows(), 4);
  

  delete h;
  delete hi;
  delete hello;
  delete c_int;
  delete c_bool;
  delete c_float;
  delete c_string;

  exit(0);
}

void dataframe_constructor_tests() {
  Schema schema1("IFSBSB");
  DataFrame* dataframe1 = new DataFrame(schema1);
  GT_EQUALS(dataframe1->ncols(), 6);
  GT_EQUALS(dataframe1->nrows(), 0);

  DataFrame* dataframe2 = new DataFrame(*dataframe1);
  IntColumn* int_column = new IntColumn();
  GT_EQUALS(dataframe2->ncols(), 6);
  GT_EQUALS(dataframe2->nrows(), 0);
  
  // Testing to see that the copy is not linked to the original
  dataframe2->add_column(int_column, nullptr);
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
  Schema s1("");
  DataFrame df(s1);
  String* hi = new String("hi");
  String* hello = new String("hello");
  String* h = new String("h");

  GT_EQUALS(df.ncols(), 0);
  GT_EQUALS(df.nrows(), 0);

  Column* c_int = new IntColumn(4, 1, 3, 4, 2);
  Column* c_float = new FloatColumn(4, (float)1.2, (float)3.2, (float)2, (float)1);
  Column* c_string = new StringColumn(5, hi, hello, nullptr, hi, h);
  Column* c_bool = new BoolColumn(3, (bool)0, (bool)1, (bool)1);

  df.add_column(c_int, hi);
  df.add_column(c_float, hello);
  df.add_column(c_string, hi);
  df.add_column(c_bool, nullptr);

  GT_EQUALS(df.get_int(0, 0), 1);
  GT_EQUALS(df.get_int(0, 1), 3);
  GT_EQUALS(df.get_int(0, 2), 4);
  GT_EQUALS(df.get_int(0, 3), 2);

  GT_EQUALS(df.get_float(1, 0), (float)1.2);
  GT_EQUALS(df.get_float(1, 1), (float)3.2);
  GT_EQUALS(df.get_float(1, 2), (float)2);
  GT_EQUALS(df.get_float(1, 3), (float)1);

  GT_EQUALS(df.get_string(2, 0), hi);
  GT_EQUALS(df.get_string(2, 1), hello);
  GT_EQUALS(df.get_string(2, 2), nullptr);
  GT_EQUALS(df.get_string(2, 3), hi);
  GT_EQUALS(df.get_string(2, 4), h);

  GT_EQUALS(df.get_bool(3, 0), false);
  GT_EQUALS(df.get_bool(3, 1), 1);
  GT_EQUALS(df.get_bool(3, 2), true);

  // Test that this array grew to fit the row size
  GT_EQUALS(df.get_bool(3, 3), false);

  // Test that the rest of the arrays grew from the extra string array rows
  GT_EQUALS(df.get_int(0, 4), 0);
  GT_EQUALS(df.get_float(1, 4), 0);
  GT_EQUALS(df.get_bool(3, 4), 0);

  delete h;
  delete hi;
  delete hello;
  delete c_int;
  delete c_bool;
  delete c_float;
  delete c_string;

  exit(0);
}

void dataframe_setters_tests() { 
  Schema s1("");
  DataFrame df(s1);
  String* hi = new String("hi");
  String* hello = new String("hello");
  String* h = new String("h");

  GT_EQUALS(df.ncols(), 0);
  GT_EQUALS(df.nrows(), 0);

  Column* c_int = new IntColumn(4, 1, 3, 4, 2);
  Column* c_float = new FloatColumn(4, (float)1.2, (float)3.2, (float)2, (float)1);
  Column* c_string = new StringColumn(17, hi, hello, nullptr, hi, h);
  Column* c_bool = new BoolColumn(3, (bool)0, (bool)1, (bool)1);

  df.add_column(c_int, hi);
  df.add_column(c_float, hello);
  df.add_column(c_string, hi);
  df.add_column(c_bool, nullptr);

  for (size_t ii = 0; ii < df.nrows(); ii++) {
    df.set(0, ii, 14);
    df.set(1, ii, (float)12.44);
    df.set(2, ii, hello);
    df.set(3, ii, true);
  }

  for (size_t ii = 0; ii < df.nrows(); ii++) {
    GT_EQUALS(df.get_int(0, ii), 14);
    GT_EQUALS(df.get_float(1, ii), (float)12.44);
    GT_EQUALS(df.get_string(2, ii), hello);
    GT_EQUALS(df.get_bool(3, ii), true);
  }

  delete h;
  delete hi;
  delete hello;
  delete c_int;
  delete c_bool;
  delete c_float;
  delete c_string;

  exit(0);
}

void dataframe_fill_row_tests() { 
  Schema s1("");
  DataFrame df(s1);
  String* hi = new String("hi");
  String* hello = new String("hello");
  String* h = new String("h");

  GT_EQUALS(df.ncols(), 0);
  GT_EQUALS(df.nrows(), 0);

  Column* c_int = new IntColumn(4, 1, 3, 4, 2);
  Column* c_float = new FloatColumn(4, (float)1.2, (float)3.2, (float)2, (float)1);
  Column* c_string = new StringColumn(5, hi, hello, nullptr, hi, h);
  Column* c_bool = new BoolColumn(3, (bool)0, (bool)1, (bool)1);

  df.add_column(c_int, hi);
  df.add_column(c_float, hello);
  df.add_column(c_string, hi);
  df.add_column(c_bool, nullptr);

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

  GT_EQUALS(row1->get_float(1), (float)1.2);
  GT_EQUALS(row2->get_float(1), (float)3.2);
  GT_EQUALS(row3->get_float(1), (float)2);
  GT_EQUALS(row4->get_float(1), (float)1);
  GT_EQUALS(row5->get_float(1), 0);

  GT_EQUALS(row1->get_string(2), hi);
  GT_EQUALS(row2->get_string(2), hello);
  GT_EQUALS(row3->get_string(2), nullptr);
  GT_EQUALS(row4->get_string(2), hi);
  GT_EQUALS(row5->get_string(2), h);

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
  delete c_float;
  delete c_string;
  delete row1;
  delete row2;
  delete row3; 
  delete row4;
  delete row5;

  exit(0);
}

void dataframe_add_row_tests() { 
  Schema s1("");
  DataFrame df(s1);
  String* hi = new String("hi");
  String* hello = new String("hello");
  String* h = new String("h");

  GT_EQUALS(df.ncols(), 0);
  GT_EQUALS(df.nrows(), 0);

  Column* c_int = new IntColumn(4, 1, 3, 4, 2);
  Column* c_float = new FloatColumn(4, (float)1.2, (float)3.2, (float)2, (float)1);
  Column* c_string = new StringColumn(5, hi, hello, nullptr, hi, h);
  Column* c_bool = new BoolColumn(3, (bool)0, (bool)1, (bool)1);

  df.add_column(c_int, hi);
  df.add_column(c_float, hello);
  df.add_column(c_string, hi);
  df.add_column(c_bool, nullptr);

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

    GT_EQUALS(df.get_float(1, ii), (float)1.2);
    GT_EQUALS(df.get_float(1, ii + 1), (float)3.2);
    GT_EQUALS(df.get_float(1, ii + 2), (float)2);
    GT_EQUALS(df.get_float(1, ii + 3), (float)1);
    GT_EQUALS(df.get_float(1, ii + 4), 0);

    GT_EQUALS(df.get_string(2, ii), hi);
    GT_EQUALS(df.get_string(2, ii + 1), hello);
    GT_EQUALS(df.get_string(2, ii + 2), nullptr);
    GT_EQUALS(df.get_string(2, ii + 3), hi);
    GT_EQUALS(df.get_string(2, ii + 4), h);

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
  delete c_float;
  delete c_string;
  delete row1;
  delete row2;
  delete row3; 
  delete row4;
  delete row5;

  exit(0);
}

void test_pmap_add() {
  Schema s("IIFI");
  DataFrame df(s);
  Row  r(df.get_schema());
  AddRower rower(&df);

  for (size_t i = 0; i < NUM_ROWS; i++) {
    r.set(0,(int)i);
    r.set(1,(int)i+1);
    r.set(2, (float)i);
    df.add_row(r);
  }

  df.pmap(rower);

  for (size_t i = 0; i < NUM_ROWS; i++) {
    GT_EQUALS(df.get_int(3,i), i+i+1);
  }

  for (size_t i = 0; i < NUM_ROWS; i++) {
    GT_EQUALS(df.get_int((size_t)0,i), i);
    GT_EQUALS(df.get_int((size_t)1,i), i+1);
    GT_EQUALS(df.get_float((size_t)2,i), (float)i);
  }
  
  exit(0);
}

void test_map_add() {
  Schema s("IIFI");
  DataFrame df(s);
  Row  r(df.get_schema());
  AddRower rower(&df);

  for (size_t i = 0; i <  NUM_ROWS; i++) {
    r.set(0,(int)i);
    r.set(1,(int)i+1);
    r.set(2, (float)i);
    df.add_row(r);
  }

  df.map(rower);

  for (size_t i = 0; i < NUM_ROWS; i++) {
    GT_EQUALS(df.get_int(3,i), i+i+1);
  }

  for (size_t i = 0; i < NUM_ROWS; i++) {
    GT_EQUALS(df.get_int((size_t)0,i), i);
    GT_EQUALS(df.get_int((size_t)1,i), i+1);
    GT_EQUALS(df.get_float((size_t)2,i), (float)i);
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
TEST(IntColumn, int_column_constructor_tests){ ASSERT_EXIT_ZERO(int_column_constructor_tests); }
TEST(IntColumn, int_column_set_tests){ ASSERT_EXIT_ZERO(int_column_set_tests); }
TEST(FloatColumn, float_column_constructor_tests){ ASSERT_EXIT_ZERO(float_column_constructor_tests); }
TEST(FloatColumn, float_column_set_tests){ ASSERT_EXIT_ZERO(float_column_set_tests); }
TEST(BoolColumn, bool_column_constructor_tests){ ASSERT_EXIT_ZERO(bool_column_constructor_tests); }
TEST(BoolColumn, bool_column_set_tests){ ASSERT_EXIT_ZERO(bool_column_set_tests); }
TEST(StringColumn, string_column_constructor_tests){ ASSERT_EXIT_ZERO(string_column_constructor_tests); }
TEST(StringColumn, string_column_set_tests){ ASSERT_EXIT_ZERO(string_column_set_tests); }

// Schema Tests
TEST(Schema, schema_constructor_tests){ ASSERT_EXIT_ZERO(schema_constructor_tests); }
TEST(Schema, schema_add_column_tests){ ASSERT_EXIT_ZERO(schema_add_column_tests); }
TEST(Schema, schema_add_row_tests){ ASSERT_EXIT_ZERO(schema_add_row_tests); }
TEST(Schema, schema_row_name_tests){ ASSERT_EXIT_ZERO(schema_row_name_tests); }
TEST(Schema, schema_col_name_tests){ ASSERT_EXIT_ZERO(schema_col_name_tests); }
TEST(Schema, schema_col_idx_tests){ ASSERT_EXIT_ZERO(schema_col_idx_tests); }
TEST(Schema, schema_row_idx_tests){ ASSERT_EXIT_ZERO(schema_row_idx_tests); }

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

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
