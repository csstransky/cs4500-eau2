// Made by Kaylin Devchand and Cristian Stransky
#pragma once

#include "../helpers/object.h"
#include "../helpers/string.h"

#include <stdarg.h>
#include <assert.h>
#include <cstring>
#include <thread>

// Number of elements each array in the array of arrays in Column have
const int ELEMENT_ARRAY_SIZE = 100;
const int DEFAULT_INT_VALUE = 0;
const float DEFAULT_FLOAT_VALUE = 0;
const bool DEFAULT_BOOL_VALUE = 0;
const String* DEFAULT_STRING_VALUE = nullptr;
const int NUM_THREADS = 4;

class IntColumn;
class FloatColumn;
class BoolColumn;
class StringColumn;

size_t max_(size_t a, size_t b) {
  if (a > b) {
    return a;
  } 
  else {
    return b;
  }
}

size_t min_(size_t a, size_t b) {
  if (a < b) {
    return a;
  } 
  else {
    return b;
  }
}

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. */
class Column : public Object {
 public:

  char type_;
  size_t size_;
  size_t num_arrays_;
 
  /** Type converters: Return same column under its actual type, or
   *  nullptr if of the wrong type.  */
  virtual IntColumn* as_int() { return nullptr; }
  virtual BoolColumn*  as_bool() { return nullptr; }
  virtual FloatColumn* as_float() { return nullptr; }
  virtual StringColumn* as_string() { return nullptr; }
 
  /** Type appropriate push_back methods. Calling the wrong method is
    * undefined behavior. **/
  // Child classes implement these so if these get called the program should break
  virtual void push_back(int val) {
    assert(0);
  }

  virtual void push_back(bool val) {
    assert(0);
  }

  virtual void push_back(float val) {
    assert(0);
  }

  virtual void push_back(String* val) {
    assert(0);
  }
 
 /** Returns the number of elements in the column. */
  virtual size_t size() {
    return size_;
  }
 
  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
  char get_type() {
    return type_;
  }
};
 
/*************************************************************************
 * IntColumn::
 * Holds int values.
 */
class IntColumn : public Column {
 public:

  // Elements are stored as an array of int arrays where each int array holds ELEMENT_ARRAY_SIZE 
  // number of elements.
  int** elements_;

  IntColumn() {
    type_ = 'I';
    size_ = 0;
    num_arrays_ = 1;

    // Initialize elements to consist of one array even if there are no elements in it.
    elements_ = new int*[1];
    elements_[0] = new int[ELEMENT_ARRAY_SIZE];
  }

  // NOTE: It seems that there's no offical comments about this, but if you give 'n' below the 
  // actual number of arguments (ex: IntColumn(12, 1)) then everything will be filled up to 'n' 
  // elements, but those extra elements will be completely random values. What's worse is that
  // there's basically no way to error check because those values are completely random.
  IntColumn(int n, ...) {
    type_ = 'I';
    size_ = n;

    // Determine the number of arrays needed and construct them
    num_arrays_ = n / ELEMENT_ARRAY_SIZE + 1;
    elements_ = new int*[num_arrays_];
    for (size_t i = 0; i < num_arrays_; i++) {
      elements_[i] = new int[ELEMENT_ARRAY_SIZE];
    }

    // Fill the arrays with the arguments passed in
    va_list args;
    va_start(args, n);
    for (size_t i = 0; i < n; i++) {
      size_t array = i / ELEMENT_ARRAY_SIZE;
      size_t index = i % ELEMENT_ARRAY_SIZE;

      elements_[array][index] = va_arg(args, int);
    }
    va_end(args);
  }

  ~IntColumn() {
    for (size_t i = 0; i < num_arrays_; i++) {
      delete[] elements_[i];
    }

    delete[] elements_;
  }

  int get(size_t idx) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;

    return elements_[array][index];
  }

  IntColumn* as_int() { return this; }


  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, int val) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;

    elements_[array][index] = val;
  }

  size_t size() {
    // Parent provides all the implementation needed.
    return Column::size();
  }

  // Private method to increase the number of arrays
  void increase_array_() {
    // Allocate new array
    int** new_elements = new int*[num_arrays_ * 2];
    // Copy existing arrays to new arrays
    for (size_t i = 0; i < num_arrays_; i++) {
      new_elements[i] = elements_[i];
    }

    // Allocate new arrays for the rest of the newly created arrays
    for (size_t i = num_arrays_; i < num_arrays_ * 2; i++) {
      new_elements[i] = new int[ELEMENT_ARRAY_SIZE];
    }

    delete[] elements_;
    elements_ = new_elements;
    num_arrays_ *= 2;
  }

  void push_back(int val) {
    if (num_arrays_ * ELEMENT_ARRAY_SIZE <= size_) {
      increase_array_();
    }

    size_t array = size_ / ELEMENT_ARRAY_SIZE;
    size_t index = size_ % ELEMENT_ARRAY_SIZE;

    elements_[array][index] = val;
    size_++;
  }
};
 
/*************************************************************************
 * FloatColumn::
 * Holds float values.
 */
class FloatColumn : public Column {
  public:
  // Elements are stored as an array of int arrays where each int array holds ELEMENT_ARRAY_SIZE 
  // number of elements.
  float** elements_;

  FloatColumn() {
    type_ = 'F';
    size_ = 0;
    num_arrays_ = 1;

    // Initialize elements to consist of one array even if there are no elements in it.
    elements_ = new float*[1];
    elements_[0] = new float[ELEMENT_ARRAY_SIZE];
  }

  // NOTE: It seems that there's no offical comments about this, but if you give 'n' below the 
  // actual number of arguments (ex: FloatColumn(12, 1.0)) then everything will be filled up to 'n' 
  // elements, but those extra elements will be completely random values. What's worse is that
  // there's basically no way to error check because those values are completely random.
  FloatColumn(int n, ...) {
    type_ = 'F';
    size_ = n;

    // Determine the number of arrays needed and construct them
    num_arrays_ = n / ELEMENT_ARRAY_SIZE + 1;
    elements_ = new float*[num_arrays_];
    for (size_t i = 0; i < num_arrays_; i++) {
      elements_[i] = new float[ELEMENT_ARRAY_SIZE];
    }

    // Fill the arrays with the arguments passed in
    va_list args;
    va_start(args, n);
    for (size_t i = 0; i < n; i++) {
      size_t array = i / ELEMENT_ARRAY_SIZE;
      size_t index = i % ELEMENT_ARRAY_SIZE;

      elements_[array][index] = va_arg(args, double);
    }
    va_end(args);
  }

  ~FloatColumn() {
    for (size_t i = 0; i < num_arrays_; i++) {
      delete[] elements_[i];
    }

    delete[] elements_;
  }

  float get(size_t idx) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;

    return elements_[array][index];
  }

  FloatColumn* as_float() { return this; }


  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, float val) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;

    elements_[array][index] = val;
  }

  size_t size() {
    // Parent provides all the implementation needed.
    return Column::size();
  }

  // Private method to increase the number of arrays
  void increase_array_() {
    // Allocate new array
    float** new_elements = new float*[num_arrays_ * 2];
    // Copy existing arrays to new arrays
    for (size_t i = 0; i < num_arrays_; i++) {
      new_elements[i] = elements_[i];
    }

    // Allocate new arrays for the rest of the newly created arrays
    for (size_t i = num_arrays_; i < num_arrays_ * 2; i++) {
      new_elements[i] = new float[ELEMENT_ARRAY_SIZE];
    }

    delete[] elements_;
    elements_ = new_elements;
    num_arrays_ *= 2;
  }

  void push_back(float val) {
    if (num_arrays_ * ELEMENT_ARRAY_SIZE <= size_) {
      increase_array_();
    }

    size_t array = size_ / ELEMENT_ARRAY_SIZE;
    size_t index = size_ % ELEMENT_ARRAY_SIZE;

    elements_[array][index] = val;
    size_++;
  }
};

/*************************************************************************
 * BoolColumn::
 * Holds Boolean values.
 */
class BoolColumn : public Column {
 public:
   // Elements are stored as an array of bool arrays where each bool array holds ELEMENT_ARRAY_SIZE 
  // number of elements.
  bool** elements_;

  BoolColumn() {
    type_ = 'B';
    size_ = 0;
    num_arrays_ = 1;

    // Initialize elements to consist of one array even if there are no elements in it.
    elements_ = new bool*[1];
    elements_[0] = new bool[ELEMENT_ARRAY_SIZE];
  }

  // NOTE: It seems that there's no offical comments about this, but if you give 'n' below the 
  // actual number of arguments (ex: BoolColumn(12, 1)) then everything will be filled up to 'n' 
  // elements, but those extra elements will be completely random values. What's worse is that
  // there's basically no way to error check because those values are completely random.
  BoolColumn(int n, ...) {
    type_ = 'B';
    size_ = n;

    // Determine the number of arrays needed and construct them
    num_arrays_ = n / ELEMENT_ARRAY_SIZE + 1;
    elements_ = new bool*[num_arrays_];
    for (size_t i = 0; i < num_arrays_; i++) {
      elements_[i] = new bool[ELEMENT_ARRAY_SIZE];
    }

    // Fill the arrays with the arguments passed in
    va_list args;
    va_start(args, n);
    for (size_t i = 0; i < n; i++) {
      size_t array = i / ELEMENT_ARRAY_SIZE;
      size_t index = i % ELEMENT_ARRAY_SIZE;

      elements_[array][index] = va_arg(args, int);
    }
    va_end(args);
  }

  ~BoolColumn() {
    for (size_t i = 0; i < num_arrays_; i++) {
      delete[] elements_[i];
    }

    delete[] elements_;
  }

  bool get(size_t idx) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;

    return elements_[array][index];
  }

  BoolColumn* as_bool() { return this; }


  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, bool val) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;

    elements_[array][index] = val;
  }

  size_t size() {
    // Parent provides all the implementation needed.
    return Column::size();
  }

  // Private method to increase the number of arrays
  void increase_array_() {
    // Allocate new array
    bool** new_elements = new bool*[num_arrays_ * 2];
    // Copy existing arrays to new arrays
    for (size_t i = 0; i < num_arrays_; i++) {
      new_elements[i] = elements_[i];
    }

    // Allocate new arrays for the rest of the newly created arrays
    for (size_t i = num_arrays_; i < num_arrays_ * 2; i++) {
      new_elements[i] = new bool[ELEMENT_ARRAY_SIZE];
    }

    delete[] elements_;
    elements_ = new_elements;
    num_arrays_ *= 2;
  }

  void push_back(bool val) {
    if (num_arrays_ * ELEMENT_ARRAY_SIZE <= size_) {
      increase_array_();
    }

    size_t array = size_ / ELEMENT_ARRAY_SIZE;
    size_t index = size_ % ELEMENT_ARRAY_SIZE;

    elements_[array][index] = val;
    size_++;
  }
};
 
/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
class StringColumn : public Column {
 public:
   // Elements are stored as an array of int arrays where each int array holds ELEMENT_ARRAY_SIZE 
  // number of elements.
  String*** elements_;

  StringColumn() {
    type_ = 'S';
    size_ = 0;
    num_arrays_ = 1;

    // Initialize elements to consist of one array even if there are no elements in it.
    elements_ = new String**[1];
    elements_[0] = new String*[ELEMENT_ARRAY_SIZE];
  }

  // NOTE: It seems that there's no offical comments about this, but if you give 'n' below the 
  // actual number of arguments (ex: StringColumn(12, str)) then everything will be filled up to 'n' 
  // elements, but those extra elements will be completely random values. What's worse is that
  // there's basically no way to error check because those values are completely random.
  // DANGER: Because of the above behavior, this WILL NOT WORK if n is larger than the actual 
  // elemnents because each String that's passed in is cloned.
  StringColumn(int n, ...) {
    type_ = 'S';
    size_ = n;

    // Determine the number of arrays needed and construct them
    num_arrays_ = n / ELEMENT_ARRAY_SIZE + 1;
    elements_ = new String**[num_arrays_];
    for (size_t i = 0; i < num_arrays_; i++) {
      elements_[i] = new String*[ELEMENT_ARRAY_SIZE];
    }

    // Fill the arrays with the arguments passed in
    va_list args;
    va_start(args, n);
    for (size_t i = 0; i < n; i++) {
      size_t array = i / ELEMENT_ARRAY_SIZE;
      size_t index = i % ELEMENT_ARRAY_SIZE;

      String* temp_string = va_arg(args, String*);
      elements_[array][index] = temp_string ? temp_string->clone() : nullptr;
    }
    va_end(args);
  }

  ~StringColumn() {
    size_t count = 0;
    for (size_t i = 0; i < num_arrays_; i++) {
      for (size_t j = 0; j < ELEMENT_ARRAY_SIZE && count < size_; j++) {
        if (elements_[i][j]) {
          delete elements_[i][j];
        }
        count++;
      }
      delete[] elements_[i];
    }
    delete[] elements_;
  }

  String* get(size_t idx) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;

    return elements_[array][index];
  }

  StringColumn* as_string() { return this; }


  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, String* val) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;

    if (elements_[array][index]) {
      delete elements_[array][index];
    }
    
    elements_[array][index] = val ? val->clone() : nullptr;
  }

  size_t size() {
    // Parent provides all the implementation needed.
    return Column::size();
  }

  // Private method to increase the number of arrays
  void increase_array_() {
    // Allocate new array
    String*** new_elements = new String**[num_arrays_ * 2];
    // Copy existing arrays to new arrays
    for (size_t i = 0; i < num_arrays_; i++) {
      new_elements[i] = elements_[i];
    }

    // Allocate new arrays for the rest of the newly created arrays
    for (size_t i = num_arrays_; i < num_arrays_ * 2; i++) {
      new_elements[i] = new String*[ELEMENT_ARRAY_SIZE];
    }

    delete[] elements_;
    elements_ = new_elements;
    num_arrays_ *= 2;
  }

  void push_back(String* val) {
    if (num_arrays_ * ELEMENT_ARRAY_SIZE <= size_) {
      increase_array_();
    }

    size_t array = size_ / ELEMENT_ARRAY_SIZE;
    size_t index = size_ % ELEMENT_ARRAY_SIZE;

    elements_[array][index] = val ? val->clone() : nullptr;
    size_++;
  }
};
 
 
/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object {
 public:
  char* types_;
  size_t num_cols_;
  size_t num_rows_;
  size_t col_array_size_;

  /** Copying constructor */
  Schema(Schema& from) : Schema(from.types_) {
    // Since these are primitive types, we can get away with this
    this->num_rows_ = from.num_rows_;
  }
 
  /** Create an empty schema **/
  Schema() {
    col_array_size_ = 1;
    types_ = new char[col_array_size_];
    types_[0] = '\0';
    num_cols_ = 0;
    num_rows_ = 0;
  }
 
  /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr argument is
    * undefined. **/
  Schema(const char* types) {
    assert(types != nullptr);
    size_t types_length = strlen(types);
    for (size_t ii = 0; ii < types_length; ii++) {
      assert(types[ii] == 'I' || types[ii] == 'F' || types[ii] == 'B' || types[ii] == 'S');
    }

    col_array_size_ = types_length + 1;
    this->types_ = new char[col_array_size_];
    strcpy(types_, types);
    this->types_[types_length] = '\0';
    // strlen doesn't include the last '\0' char so this will return the exact number of columns
    num_cols_ = types_length; 
    num_rows_ = 0;
  }

  ~Schema() {
    delete[] types_;
  }

  /**
   * Helper function that will increase BOTH the types and col_names arrays (as they're both linked)
   */
  void increase_size_column_arrays_() {
    this->col_array_size_ = col_array_size_ * 2;

    char* new_types = new char[col_array_size_];
    strcpy(new_types, types_);
    new_types[num_cols_ + 1] = '\0';
    delete[] types_;
    this->types_ = new_types;
  }

  /** Add a column of the given type and name (can be nullptr), name
    * is external. Names are expectd to be unique, duplicates result
    * in undefined behavior. */
  void add_column(char typ) {
    assert(typ == 'I' || typ == 'F' || typ == 'B' || typ == 'S');
    if (num_cols_ + 1 >= col_array_size_) {
      increase_size_column_arrays_();
    }
    this->types_[num_cols_] = typ;
    this->types_[num_cols_ + 1] = '\0';
    this->num_cols_++;
  }

 
  /** Add a row with a name (possibly nullptr), name is external.  Names are
   *  expectd to be unique, duplicates result in undefined behavior. */
  void add_row() {
    this->num_rows_++;
  }

  /** Return type of column at idx. An idx >= width is undefined. */
  char col_type(size_t idx) {
    assert(idx < num_cols_);
    return this->types_[idx];
  }
 
  /** The number of columns */
  size_t width() {
    return num_cols_;
  }
 
  /** The number of rows */
  size_t length() {
    return num_rows_;
  }
};
 
/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object {
public:

  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r) = 0;
 
  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b) = 0;

  virtual void accept(float f) = 0;

  virtual void accept(int i) = 0;

  virtual void accept(String* s) = 0;
 
  /** Called when all fields have been seen. */
  virtual void done() = 0;
  
};

/** 
 * Helper function to get an absolutely certain BoolColumn with error checking. 
 * Throws error if the col_idx is out of bounds, or if the type of the Column is not BoolColumn.
 */
BoolColumn* get_bool_column_(Column** columns, size_t columns_size, size_t col_idx) {
  assert(col_idx < columns_size);
  BoolColumn* bool_column = columns[col_idx]->as_bool();
  assert(bool_column);
  return bool_column;
}

/** 
 * Helper function to get an absolutely certain IntColumn with error checking. 
 * Throws error if the col_idx is out of bounds, or if the type of the Column is not IntColumn.
 */
IntColumn* get_int_column_(Column** columns, size_t columns_size, size_t col_idx) {
  assert(col_idx < columns_size);
  IntColumn* int_column = columns[col_idx]->as_int();
  assert(int_column);
  return int_column;
}

/** 
 * Helper function to get an absolutely certain FloatColumn with error checking. 
 * Throws error if the col_idx is out of bounds, or if the type of the Column is not FloatColumn.
 */
FloatColumn* get_float_column_(Column** columns, size_t columns_size, size_t col_idx) {
  assert(col_idx < columns_size);
  FloatColumn* float_column = columns[col_idx]->as_float();
  assert(float_column);
  return float_column;
}

/** 
 * Helper function to get an absolutely certain StringColumn with error checking. 
 * Throws error if the col_idx is out of bounds, or if the type of the Column is not StringColumn.
 */
StringColumn* get_string_column_(Column** columns, size_t columns_size, size_t col_idx) {
  assert(col_idx < columns_size);
  StringColumn* string_column = columns[col_idx]->as_string();
  assert(string_column);
  return string_column;
}

/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 */
class Row : public Object {
  public:

  // list of columns with only one element each
  Column** cols_;
  size_t idx_;
  size_t width_;
 
  /** Build a row following a schema. */
  Row(Schema& scm) {
    width_ = scm.width();
    cols_ = new Column*[width_];

    for (size_t i = 0; i < width_; i++) {
      switch (scm.col_type(i)) {
        case 'I':
          cols_[i] = new IntColumn(1, DEFAULT_INT_VALUE);
          break;
        case 'F':
          cols_[i] = new FloatColumn(1, DEFAULT_FLOAT_VALUE);
          break;
        case 'B':
          cols_[i] = new BoolColumn(1, DEFAULT_BOOL_VALUE);
          break;
        case 'S':
          cols_[i] = new StringColumn(1, (String*)DEFAULT_STRING_VALUE);
          break;
        default:
          // Shoulld never reach here 
          assert(0);
      }
    }

    // Default value of idx
    idx_ = SIZE_MAX;
  }
  
  ~Row() {
    for (size_t ii = 0; ii < width_; ii++) {
      Column* column = cols_[ii];
      delete column;
    }
    delete[] cols_;
  }
 
  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
  void set(size_t col, int val) {
    IntColumn* column = get_int_column_(this->cols_, this->width_, col);
    column->set(0, val);
  }

  void set(size_t col, float val) {
    FloatColumn* column = get_float_column_(this->cols_, this->width_, col);
    column->set(0, val);
  }

  void set(size_t col, bool val) {
    BoolColumn* column = get_bool_column_(this->cols_, this->width_, col);
    column->set(0, val);
  }

  /** Acquire ownership of the string. */
  void set(size_t col, String* val) {
    StringColumn* column = get_string_column_(this->cols_, this->width_, col);
    column->set(0, val);
  }
 
  /** Set/get the index of this row (ie. its position in the dataframe. This is
   *  only used for informational purposes, unused otherwise */
  void set_idx(size_t idx) {
    idx_ = idx;
  }

  /** If never set, returns SIZE_MAX */
  size_t get_idx() {
    return idx_;
  }
 
  /** Getters: get the value at the given column. If the column is not
    * of the requested type, the result is undefined. */
  int get_int(size_t col) {
    IntColumn* column = get_int_column_(this->cols_, this->width_, col);
    return column->get(0);
  }

  bool get_bool(size_t col) {
    BoolColumn* column = get_bool_column_(this->cols_, this->width_, col);
    return column->get(0);
  }

  float get_float(size_t col) {
    FloatColumn* column = get_float_column_(this->cols_, this->width_, col);
    return column->get(0);
  }
  
  String* get_string(size_t col) {
    StringColumn* column = get_string_column_(this->cols_, this->width_, col);
    return column->get(0);
  }
 
  /** Number of fields in the row. */
  size_t width() {
    return width_;
  }
 
   /** Type of the field at the given position. An idx >= width is  undefined. */
  char col_type(size_t idx) {
    return cols_[idx]->get_type();
  }

 
  /** Given a Fielder, visit every field of this row. The first argument is
    * index of the row in the dataframe.
    * Calling this method before the row's fields have been set is undefined. */
  void visit(size_t idx, Fielder& f) {
    f.start(idx);
    for (size_t i = 0; i < width_; i++) {
      switch (cols_[i]->get_type()) {
        case 'I':
          f.accept(get_int(i));
          break;
        case 'F':
          f.accept(get_float(i));
          break;
        case 'B':
          f.accept(get_bool(i));
          break;
        case 'S':
          f.accept(get_string(i));
          break;
        default:
          // Should never reach here 
          assert(0);
      }
    }
    f.done();
  }
 
};
 
/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 */
class Rower : public Object {
 public:
  /** This method is called once per row. The row object is on loan and
      should not be retained as it is likely going to be reused in the next
      call. The return value is used in filters to indicate that a row
      should be kept. */
  virtual bool accept(Row& r) = 0;
 
  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  virtual void join_delete(Rower* other) = 0;
};

/*****************************************************************************
 * PrinterFielder::
 * Prints out each field in the row.
 */
class PrinterFielder : public Fielder {
public:

  PrinterFielder() {

  }

  ~PrinterFielder() {
    
  }
 
  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  void start(size_t r) {

  }
 
  /** Called for fields of the argument's type with the value of the field. */
  void accept(bool b) {
    printf("<%d> ", b);
  }

  void accept(float f) {
    printf("<%f> ", f);
  }

  void accept(int i) {
    printf("<%d> ", i);
  }
  
  void accept(String* s) {
    if (s == nullptr) {
      printf("<> ");
    }
    else {
      printf("<%s> ", s->c_str());
    }
  }
 
  /** Called when all fields have been seen. */
  void done() {
    printf("\n");
  }
};

/*******************************************************************************
 *  PrinterRower::
 *  A Rower to print all fields in the row.
 */
class PrinterRower : public Rower {
 public:
  PrinterFielder* fielder_;

  PrinterRower() {
    fielder_ = new PrinterFielder();
  }

  PrinterRower(PrinterFielder* fielder) {
    fielder_ = fielder;
  }

  ~PrinterRower() {
    delete fielder_;
  }

    /** Return a copy of the object; nullptr is considered an error */
  Object* clone() {
    return new PrinterRower(fielder_);
  }

  /** Returns false if the row has all empty fields. */
  bool accept(Row& r) {
    r.visit(0, *fielder_);
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

 
/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame : public Object {
 public:
  Schema schema_;
  Column** cols_;
  size_t col_array_size_;

  /**
   * Helper function that will make a NEW Column of a specific type. This is mainly used for
   * our constructors to abstract away our switch case.
   * NOTE: Make sure to properly delete these guys later as you are making NEW Columns
   */
  Column* make_new_column_(char col_type) {
    switch (col_type) {
      case 'I': 
        return new IntColumn();
      case 'F': 
        return new FloatColumn();
      case 'B': 
        return new BoolColumn();
      case 'S': 
        return new StringColumn();
      default:
        assert(0); // should never reach here
    }
  }
 
  /** Create a data frame from a schema and columns. All columns are created
    * empty. */
  DataFrame(Schema& schema) {
    size_t num_cols = schema.width();
    // It's possible to make a schema with 0 columns, so we want to make sure we have atleast an 
    // array size of 1, or else our doubling in size arthimetic won't work correctly
    this->col_array_size_ = max_(num_cols, 1);
    this->cols_ = new Column*[this->col_array_size_];
    
    for (size_t ii = 0; ii < num_cols; ii++) {
        char col_type = schema.col_type(ii);
        this->schema_.add_column(col_type);
        this->cols_[ii] = make_new_column_(col_type);
    }
  }

  /** Create a data frame with the same columns as the given df but with no rows or rownmaes */
  DataFrame(DataFrame& df) : DataFrame(df.get_schema()) {
 
  }
  
  ~DataFrame() {
    for (size_t ii = 0; ii < this->schema_.width(); ii++) {
      Column* column = this->cols_[ii];
      delete column;
    }
    delete[] this->cols_;
  }
 
  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema& get_schema() {
    return this->schema_;
  }

  /**
   * Helper function that will fill the given Column with default values until a given
   * num_rows_to_fill values. Usually used to fill a column that needs to reach a certain size so 
   * that the dataframe can have Columns of equal size.
   */
  void fill_rest_of_column_with_empty_values_(Column* column, size_t num_rows_to_fill) {
    char column_type = column->get_type();
    // The for loops are inside the switch cases instead of just having 1 for loop outside the 
    // switch case for a little more efficiency by avoiding unnecessary checks every iteration
    switch (column_type) {
      case 'I': {
        for (size_t jj = 0; jj < num_rows_to_fill; jj++) {
          column->push_back(DEFAULT_INT_VALUE);
        }
        break;
      }
      case 'F': {
        for (size_t jj = 0; jj < num_rows_to_fill; jj++) {
          column->push_back(DEFAULT_FLOAT_VALUE);
        }
        break;
      }
      case 'B': {
        for (size_t jj = 0; jj < num_rows_to_fill; jj++) {
          column->push_back(DEFAULT_BOOL_VALUE);
        }
        break;
      }
      case 'S': {
        for (size_t jj = 0; jj < num_rows_to_fill; jj++) {
          column->push_back((String*)DEFAULT_STRING_VALUE);
        }
        break;
      }
      default:
        // Schema should always have a type between 'IFBS', otherwise an error is thrown
        assert(0);
    }
  }

  /** Helper that makes a copied column super easy */
  Column* make_new_copy_column_(Column* col) {
    Column* copy_column;
    char col_type = col->get_type();
    size_t col_size = col->size();
    switch (col_type) {
      case 'I': {
        copy_column = new IntColumn();
        for (size_t ii = 0; ii < col_size; ii++) {
          copy_column->push_back(col->as_int()->get(ii));
        }
        break;
      }
      case 'F': {
        copy_column = new FloatColumn();
        for (size_t ii = 0; ii < col_size; ii++) {
          copy_column->push_back(col->as_float()->get(ii));
        }
        break;
      }
      case 'B': {
        copy_column = new BoolColumn();
        for (size_t ii = 0; ii < col_size; ii++) {
          copy_column->push_back(col->as_bool()->get(ii));
        }
        break;
      }
      case 'S': {
        copy_column = new StringColumn();
        for (size_t ii = 0; ii < col_size; ii++) {
          copy_column->push_back(col->as_string()->get(ii));
        }
        break;
      }
      default:
        assert(0);
    }
    return copy_column;
  }

  /** 
   * Helper function that will increase the size of the cols_ and col_array_size_
   */
  void increase_size_column_array_() {
    this->col_array_size_ = this->col_array_size_ * 2;
    size_t num_cols = this->schema_.width();
    Column** new_cols = new Column*[this->col_array_size_];
    for (size_t ii = 0; ii < num_cols; ii++) {
      new_cols[ii] = this->cols_[ii];
    }
    delete[] this->cols_;
    this->cols_ = new_cols;
  }
 
  /** Adds a column this dataframe, updates the schema, the new column
    * is external, and appears as the last column of the dataframe, the
    * name is optional and external. A nullptr colum is undefined. */
  // NOTE: We are making a NEW copy of the col being passed in, BUT all columns will be deleted 
  // later by the DataFrame deconstructor
  void add_column(Column* col) {
    assert(col != nullptr);
    size_t num_rows = this->schema_.length();
    size_t num_cols = this->schema_.width();
    size_t col_size = col->size();

    Column* copy_column = make_new_copy_column_(col);

    // We want to make sure that every single Column has the SAME amount of rows
    if (col_size < num_rows) {
      // If the column you are adding has less rows than the dataframe,
      // fill the rest of the column with empty values.
      fill_rest_of_column_with_empty_values_(copy_column, num_rows - col_size);
    }
    else if (col_size > num_rows) {
      // If the column you are adding has more rows than the dataframe,
      // fill the dataframe with empty rows
      for (size_t ii = 0; ii < num_cols; ii++) {
        fill_rest_of_column_with_empty_values_(this->cols_[ii], col_size - num_rows);
      }
      for (size_t jj = 0; jj < col_size - num_rows; jj++) {
        this->schema_.add_row();
      }
    }

    if (num_cols + 1 >= this->col_array_size_) {
      increase_size_column_array_();
    }

    this->cols_[num_cols] = copy_column;
    this->schema_.add_column(col->get_type());
  }

  /** Gets a specific Column inside of the DataFrame. */
  Column* get_column(size_t col) {
    return this->cols_[col];
  }
 
  /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
  int get_int(size_t col, size_t row) {
    IntColumn* int_column = get_int_column_(this->cols_, this->schema_.width(), col);
    return int_column->get(row);
  }
  bool get_bool(size_t col, size_t row) {
    BoolColumn* bool_column = get_bool_column_(this->cols_, this->schema_.width(), col);
    return bool_column->get(row);
  }
  float get_float(size_t col, size_t row) {
    FloatColumn* float_column = get_float_column_(this->cols_, this->schema_.width(), col);
    return float_column->get(row);
  }
  String* get_string(size_t col, size_t row) {
    StringColumn* string_column = get_string_column_(this->cols_, this->schema_.width(), col);
    return string_column->get(row);
  }

  /** Set the value at the given column and row to the given value.
    * If the column is not  of the right type or the indices are out of
    * bound, the result is undefined. */
  void set(size_t col, size_t row, int val) {
    IntColumn* int_column = get_int_column_(this->cols_, this->schema_.width(), col);
    int_column->set(row, val);
  }
  void set(size_t col, size_t row, bool val) {
    BoolColumn* bool_column = get_bool_column_(this->cols_, this->schema_.width(), col);
    bool_column->set(row, val);
  }
  void set(size_t col, size_t row, float val) {
    FloatColumn* float_column = get_float_column_(this->cols_, this->schema_.width(), col);
    float_column->set(row, val);
  }
  void set(size_t col, size_t row, String* val) {
    StringColumn* string_column = get_string_column_(this->cols_, this->schema_.width(), col);
    string_column->set(row, val);
  }
 
  /** Set the fields of the given row object with values from the columns at
    * the given offset.  If the row is not form the same schema as the
    * dataframe, results are undefined.
    */
  void fill_row(size_t idx, Row& row) {
    assert(idx < this->schema_.length());

    size_t num_cols = this->schema_.width();
    for (size_t ii = 0; ii < num_cols; ii++) {
      char row_type = row.col_type(ii);
      char schema_type = this->schema_.col_type(ii);
      assert(row_type == schema_type);

      switch (row_type) {
        case 'I': {
          IntColumn* int_column = this->cols_[ii]->as_int();
          int int_value = int_column->get(idx);
          row.set(ii, int_value);
          break;
        }
        case 'F': {
          FloatColumn* float_column = this->cols_[ii]->as_float();
          float float_value = float_column->get(idx);
          row.set(ii, float_value);
          break;
        }
        case 'B': {
          BoolColumn* bool_column = this->cols_[ii]->as_bool();
          bool bool_value = bool_column->get(idx);
          row.set(ii, bool_value);
          break;
        }
        case 'S': {
          StringColumn* string_column = this->cols_[ii]->as_string();
          String* string_value = string_column->get(idx);
          row.set(ii, string_value);
          break;
        }
        default:
          assert(0); // should not reach this
      } 
    }
  }
 
  /** Add a row at the end of this dataframe. The row is expected to have
   *  the right schema and be filled with values, otherwise undedined.  */
  void add_row(Row& row) {
    size_t num_cols = this->schema_.width();
    assert(num_cols > 0);
    for (size_t ii = 0; ii < num_cols; ii++) {
      char row_type = row.col_type(ii);
      char schema_type = this->schema_.col_type(ii);
      assert(row_type == schema_type);

      Column* column = this->cols_[ii];
      switch (row_type) {
        case 'I': {
          int int_value = row.get_int(ii);
          column->push_back(int_value);
          break;
        }
        case 'F': {
          float float_value = row.get_float(ii);
          column->push_back(float_value);
          break;
        }
        case 'B': {
          bool bool_value = row.get_bool(ii);
          column->push_back(bool_value);
          break;
        }
        case 'S': {
          String* string_value = row.get_string(ii);
          column->push_back(string_value);
          break;
        }
        default:
          assert(0); // should not reach this
      } 
    }
    this->schema_.add_row();
  }
 
  /** The number of rows in the dataframe. */
  size_t nrows() {
    return this->schema_.length();
  }
 
  /** The number of columns in the dataframe.*/
  size_t ncols() {
    return this->schema_.width();
  }
 
  /** Visit rows in order */
  void map(Rower& r) {
    size_t num_rows = this->schema_.length();
    Row* row = new Row(this->schema_);
    for (size_t ii = 0; ii < num_rows; ii++) {
      this->fill_row(ii, *row);
      row->set_idx(ii);
      r.accept(*row);
    }
    delete row;
  }
 
  /** Create a new dataframe, constructed from rows for which the given Rower
    * returned true from its accept method. */
  DataFrame* filter(Rower& r) {
    DataFrame* filtered_dataframe = new DataFrame(*this);
    size_t num_rows = this->schema_.length();
    Row* row = new Row(this->schema_);
    for (size_t ii = 0; ii < num_rows; ii++) {
      this->fill_row(ii, *row);
      if (r.accept(*row)) {
        filtered_dataframe->add_row(*row);
      }
    }
    delete row;
    return filtered_dataframe;
  }
 
  /** Print the dataframe in SoR format to standard output. */
  void print() {
    PrinterRower* print_rower = new PrinterRower();
    this->map(*print_rower);
    delete print_rower;
  }

  /** call map on the rows from start to end, not include the endth row */
  void thread_map_(size_t start, size_t end, Rower& r) {
    Row* row = new Row(this->schema_);
    for (size_t i = start; i < end; i++) {
      this->fill_row(i, *row);
      row->set_idx(i);
      r.accept(*row);
    }
    delete row;

  }

  /** This method clones the Rower and executes the map in parallel. Join is
  * used at the end to merge the results. */
  void pmap(Rower& rower) {
    std::thread* threads[NUM_THREADS];
    Rower* clones[NUM_THREADS];

    size_t num_rows = nrows();
    // Add 1 to get ceiling of dividing size_t
    size_t rows_per_thread = num_rows / NUM_THREADS;
    if (num_rows % NUM_THREADS != 0) {
      rows_per_thread++;
    } 

    // For each thread, clone rower and create thread
    for (size_t i = 0; i < NUM_THREADS; i++) {
      clones[i] = static_cast<Rower*>(rower.clone());
      size_t start = rows_per_thread * i;
      // Minimum of the rows_per_thread and the leftover
      size_t end = min_(rows_per_thread * (i + 1), num_rows);
      threads[i] = new std::thread(&DataFrame::thread_map_, this, start, end, std::ref(*clones[i]));
    } 

    // join threads and delete rower clones
    for (size_t i = 0; i < NUM_THREADS; i++) {
      threads[i]->join();
      delete threads[i];
      rower.join_delete(clones[i]);
    }
  }
};
