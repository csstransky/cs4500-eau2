#include <sys/wait.h>

#define TEST

#include "../src/dataframe/dataframe.h" 
#include "../src/kv_store/kd_store.h"
#include "../src/application/word_count_rowers.h"
#include "../src/networks/rendezvous_server.h"
#include "../src/dataframe/column.h"

#define GT_TRUE(a)   assert(a)
#define GT_FALSE(a)  assert(!a)
#define GT_EQUALS(a, b)   assert(a == b)

const int NUM_ROWS = 1000;

/*******************************************************************************
 *  AddRower::
 *  A Rower to add a constant to every int. 
 *  NOTE: Just an example used for testing map.
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
  
  DataFrameBuilder df_b("II", &c, &kv);
  Row r(s);
  for (size_t i = 0; i <  1000 * 1000; i++) {
    r.set(0,(int)i);
    r.set(1,(int)i+1);
    df_b.add_row(r);
  }
  DataFrame* df = df_b.done();
  GT_EQUALS(df->get_int((size_t)0,1), 1);
  delete df;
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
  DataFrameBuilder df_b("IIDI", &c, &kv);
  Row* r = new Row(*s);
  int actual = 0;
  AddRower* rower = new AddRower(actual);

  for (size_t i = 0; i <  NUM_ROWS; i++) {
    r->set(0,(int)i);
    r->set(1,(int)i+1);
    r->set(2, (double)i);
    df_b.add_row(*r);
  }

  DataFrame* df = df_b.done();

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
  DataFrameBuilder df_b("IDSB", &c, &kv);
  DataFrame* df = df_b.done();

  Schema s2 = df->get_schema();

  for (int i = 0; i < 4; i++) {
    GT_EQUALS(s1->col_type(i), s2.col_type(i));
  }

  delete s1;
  delete df;

  printf("Get schema test passed!\n");
}

void dataframe_constructor_tests() {
  KV_Store kv(0);
  String c("c");
  Schema schema1("IDSBSB");
  DataFrame* dataframe1 = new DataFrame(schema1, &kv);
  GT_EQUALS(dataframe1->ncols(), 6);
  GT_EQUALS(dataframe1->nrows(), 0);

  String c_new("c_new");
  DataFrame* dataframe2 = new DataFrame(*dataframe1);
  GT_EQUALS(dataframe2->ncols(), 6);
  GT_EQUALS(dataframe2->nrows(), 0);
  
  delete dataframe1;
  delete dataframe2;

  printf("Dataframe constructor test passed!\n");
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
  assert(strcmp("IS", schema_types->c_str()) == 0);

  for (size_t ii = 0; ii < 5; ii++) {
    assert(dataframe->get_int(0, ii) == ii);
  }
  
  String string0("0xnoone");
  assert(dataframe->get_string(1, 0)->equals(&string0));

  String string1("tosch");
  assert(dataframe->get_string(1, 1)->equals(&string1));

  String string2("jmettraux");
  assert(dataframe->get_string(1, 2)->equals(&string2));

  String string3("SMGNMSKD");
  assert(dataframe->get_string(1, 3)->equals(&string3));

  String string4("kennethkalmer");
  assert(dataframe->get_string(1, 4)->equals(&string4));

  delete dataframe;

  dataframe = kd.get(&k);

  schema_types = dataframe->get_schema().types_;
  assert(strcmp("IS", schema_types->c_str()) == 0);

  for (size_t ii = 0; ii < 5; ii++) {
    assert(dataframe->get_int(0, ii) == ii);
  }
  
  assert(dataframe->get_string(1, 0)->equals(&string0));
  assert(dataframe->get_string(1, 1)->equals(&string1));
  assert(dataframe->get_string(1, 2)->equals(&string2));
  assert(dataframe->get_string(1, 3)->equals(&string3));
  assert(dataframe->get_string(1, 4)->equals(&string4));

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

  RServer* server = new RServer(server_ip->c_str());

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
    assert(map->get(hi)->value >= 100 && map->get(hi)->value <= 200);

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
    delete server;

    // exit
    exit(0);
  }

  // Fork to create another process
  if ((cpid[1] = fork())) {
      
  } else {
    // In child process
    sleep(0.5);
    KD_Store* kd = new KD_Store(0, client_ip2->c_str(), server_ip->c_str());
    sleep(2);

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
    assert(map->get(hi)->value >= 100 && map->get(hi)->value <= 200);

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
    delete server;

    // exit
    exit(0);
  }

  // In parent process

  // Start server 
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
  get_type_tests(); 

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
  dataframe_constructor_tests();
  test();

  // Map
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
