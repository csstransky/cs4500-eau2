// Made by Kaylin Devchand and Cristian Stransky
#pragma once

#include "../helpers/object.h"
#include "../helpers/string.h"
#include "../kv_store/kv_store.h"
#include "../kv_store/key_array.h"
#include "../helpers/array.h"
#include "column_array.h"
#include "rower.h"

#include <stdarg.h>
#include <assert.h>
#include <cstring>
#include <thread>
 
/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 * Authors: Kaylin Devchand & Cristian Stransky
 */
class Schema : public Object {
 public:
  char* types_;
  size_t num_cols_;
  size_t num_rows_;
  size_t types_size_; // size of types_, INCLUDES '\0' character

  /** Copying constructor */
  Schema(Schema& from) : Schema(from.types_) {
    // Since these are primitive types, we can get away with this
    this->num_rows_ = from.num_rows_;
  }
 
  /** Create an empty schema **/
  Schema() {
    types_size_ = 1;
    types_ = new char[types_size_];
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

    types_size_ = types_length + 1;
    this->types_ = new char[types_size_];
    strncpy(types_, types, types_size_);
    num_cols_ = types_length; 
    num_rows_ = 0;
  }

  ~Schema() {
    delete[] types_;
  }

  /** Subclasses should redefine */
  bool equals(Object* other) {
      if (other == this) return true;
      Schema* other_schema = dynamic_cast<Schema*>(other);
      return other_schema != nullptr 
          && this->num_cols_ == other_schema->num_cols_
          && this->num_rows_ == other_schema->num_rows_
          && strncmp(this->types_, other_schema->types_, this->types_size_) == 0;
  }

  /** Return a copy of the object; nullptr is considered an error */
  Schema* clone() {
    return new Schema(*this);
  }

  size_t serial_len() {
      return sizeof(size_t) // serial_length
        + sizeof(size_t) // num_cols_
        + sizeof(size_t) // num_rows_
        + sizeof(size_t) // types_size_
        + sizeof(char) * types_size_; // types_
  }

  char* serialize() {
      size_t serial_size = serial_len();
      Serializer serializer(serial_size);
      serializer.serialize_size_t(serial_size);
      serializer.serialize_size_t(num_cols_);
      serializer.serialize_size_t(num_rows_);
      serializer.serialize_size_t(types_size_);
      // Important: remove '\0' from serial size with "types_size_ - 1"
      serializer.serialize_chars(types_, types_size_ - 1);
      return serializer.get_serial();
  }

  static Schema* deserialize(char* serial) {
      Deserializer deserializer(serial);
      return deserialize(deserializer);
  }

  static Schema* deserialize(Deserializer& deserializer) {
      deserializer.deserialize_size_t(); // skip serial_length
      deserializer.deserialize_size_t(); // skip num_cols_
      size_t num_rows = deserializer.deserialize_size_t();
      size_t types_size = deserializer.deserialize_size_t();
      // Important: remove '\0' from deserial size with "types_size - 1"
      char* types = deserializer.deserialize_char_array(types_size - 1); 
      Schema* new_schema = new Schema(types);
      new_schema->num_rows_ = num_rows;
      delete[] types;
      return new_schema;
  }  

  /**
   * Helper function that will increase BOTH the types and col_names arrays (as they're both linked)
   */
  void increase_types_array_size_() {
    this->types_size_ = types_size_ * 2;
    char* new_types = new char[types_size_];
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
    if (num_cols_ + 1 >= types_size_) {
      increase_types_array_size_();
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
 
// TODO: This can actually be deleted (as per Jan's request)
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
BoolColumn* get_bool_column_(ColumnArray* columns, size_t columns_size, size_t col_idx) {
  assert(col_idx < columns_size);
  BoolColumn* bool_column = columns->get(col_idx)->as_bool();
  assert(bool_column);
  return bool_column;
}

/** 
 * Helper function to get an absolutely certain IntColumn with error checking. 
 * Throws error if the col_idx is out of bounds, or if the type of the Column is not IntColumn.
 */
IntColumn* get_int_column_(ColumnArray* columns, size_t columns_size, size_t col_idx) {
  assert(col_idx < columns_size);
  IntColumn* int_column = columns->get(col_idx)->as_int();
  assert(int_column);
  return int_column;
}

/** 
 * Helper function to get an absolutely certain FloatColumn with error checking. 
 * Throws error if the col_idx is out of bounds, or if the type of the Column is not FloatColumn.
 */
FloatColumn* get_float_column_(ColumnArray* columns, size_t columns_size, size_t col_idx) {
  assert(col_idx < columns_size);
  FloatColumn* float_column = columns->get(col_idx)->as_float();
  assert(float_column);
  return float_column;
}

/** 
 * Helper function to get an absolutely certain StringColumn with error checking. 
 * Throws error if the col_idx is out of bounds, or if the type of the Column is not StringColumn.
 */
StringColumn* get_string_column_(ColumnArray* columns, size_t columns_size, size_t col_idx) {
  assert(col_idx < columns_size);
  StringColumn* string_column = columns->get(col_idx)->as_string();
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
  ColumnArray* cols_;
  size_t idx_;
  size_t width_;
 
  /** Build a row following a schema. */
  Row(Schema& scm) {
    width_ = scm.width();
    cols_ = new ColumnArray(width_);
  
    for (size_t i = 0; i < width_; i++) {
      switch (scm.col_type(i)) {
        case 'I': {
          IntColumn temp_column;
          cols_->push(&temp_column);
          cols_->get(i)->push_back(DEFAULT_INT_VALUE);
          break;
        }
        case 'F': {
          FloatColumn temp_column;
          cols_->push(&temp_column);
          cols_->get(i)->push_back(DEFAULT_FLOAT_VALUE);
          break;
        }
        case 'B': {
          BoolColumn temp_column;
          cols_->push(&temp_column);
          cols_->get(i)->push_back(DEFAULT_BOOL_VALUE);
          break;
        }
        case 'S': {
          StringColumn temp_column;
          cols_->push(&temp_column);
          cols_->get(i)->push_back(&DEFAULT_STRING_VALUE);
          break;
        }
        default:
          // Shoulld never reach here 
          assert(0);
      }
    }

    // Default value of idx
    idx_ = SIZE_MAX;
  }
  
  ~Row() {
    delete cols_;
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
  
  // NOTE: Returns a pointer that can be volatile (if coming from KV store, will overwrite the old
  // cache String pointer, so String address CAN change later), clone if needed longer
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
    return cols_->get(idx)->get_type();
  }

 
  /** Given a Fielder, visit every field of this row. The first argument is
    * index of the row in the dataframe.
    * Calling this method before the row's fields have been set is undefined. */
  void visit(size_t idx, Fielder& f) {
    f.start(idx);
    for (size_t i = 0; i < width_; i++) {
      switch (cols_->get(i)->get_type()) {
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
 
// TODO: Remove this as it will NOT be viable in the future with 10GB of data
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
  Rower* clone() {
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
 * Authors: Kaylin Devchand & Cristian Stransky
 */
class DataFrame : public Object {
 public:
  Schema schema_;
  ColumnArray* cols_;
  String* name_; // owned
  KV_Store* kv_; // not owned

  /**
   * Helper function that will make a NEW Column of a specific type. This is mainly used for
   * our constructors to abstract away our switch case.
   * NOTE: Make sure to properly delete these guys later as you are making NEW Columns
   */
  Column* make_new_column_(char col_type, size_t index) {
    switch (col_type) {
      case 'I': 
        return new IntColumn(kv_, name_, index);
      case 'F': 
        return new FloatColumn(kv_, name_, index);
      case 'B': 
        return new BoolColumn(kv_, name_, index);
      case 'S': 
        return new StringColumn(kv_, name_, index);
      default:
        assert(0); // should never reach here
    }
  }
 
  /** Create a data frame from a schema and columns. All columns are created
    * empty. */
  DataFrame(Schema& schema, String* name, KV_Store* kv) {
    name_ = name->clone();
    kv_ = kv;
    size_t num_cols = schema.width();
    // It's possible to make a schema with 0 columns, so we want to make sure we have atleast an 
    // array size of 1, or else our doubling in size arthimetic won't work correctly
    size_t col_size = max_(num_cols, 1);
    this->cols_ = new ColumnArray(col_size);
    
    for (size_t ii = 0; ii < num_cols; ii++) {
        char col_type = schema.col_type(ii);
        this->schema_.add_column(col_type);

        Column* temp_col = make_new_column_(col_type, ii);
        this->cols_->push(temp_col);
        delete temp_col;
    }
  }

  /** copy constructor mainly used for deserialization */
  DataFrame(Schema& schema, String* name, KV_Store* kv, ColumnArray* columns) : schema_(schema) {
    this->kv_ = kv;
    this->name_ = name->clone();
    this->cols_ = columns->clone();    
  }

  /** Create a data frame with the same columns as the given df but with no rows or rownmaes */
  DataFrame(DataFrame& df, String* name) : DataFrame(df.get_schema(), name, df.kv_) {

  }
  
  ~DataFrame() {
    delete cols_;
    delete name_;
  }

  /** Subclasses should redefine */
  bool equals(Object* other) {
    DataFrame* other_df = dynamic_cast<DataFrame*>(other);
    return other_df != nullptr
      && schema_.equals(&other_df->schema_)
      && cols_->equals(other_df->cols_)
      && name_->equals(other_df->name_);
  }

  /** Return a copy of the object; nullptr is considered an error */
  DataFrame* clone() {
    return new DataFrame(schema_, name_, kv_, cols_);
  }

  size_t serial_len() {
    return sizeof(size_t) // serial_length
      + schema_.serial_len()
      + cols_->serial_len()
      + name_->serial_len();
  }

  char* serialize() {
      size_t serial_size = serial_len();
      Serializer serializer(serial_size);
      serializer.serialize_size_t(serial_size);
      serializer.serialize_object(&schema_);
      serializer.serialize_object(cols_);
      serializer.serialize_object(name_);
      return serializer.get_serial();
  }

  static DataFrame* deserialize(char* serial, KV_Store* kv_store) {
      Deserializer deserializer(serial);
      return deserialize(deserializer, kv_store);
  }

  static DataFrame* deserialize(Deserializer& deserializer, KV_Store* kv_store) {
      deserializer.deserialize_size_t(); // skip serial_length
      Schema* schema = Schema::deserialize(deserializer);
      ColumnArray* columns = ColumnArray::deserialize(deserializer, kv_store);
      String* name = String::deserialize(deserializer);

      DataFrame* new_dataframe = new DataFrame(*schema, name, kv_store, columns);
      delete schema;
      delete columns;
      delete name;
      return new_dataframe;
  }  

  // Implemented in kd_store.h to remove circular dependency. See piazza post @963
  static DataFrame* from_scalar(Key* key, KD_Store* kd, int val);

  static DataFrame* from_scalar(Key* key, KD_Store* kd, float val);

  static DataFrame* from_scalar(Key* key, KD_Store* kd, bool val);

  static DataFrame* from_scalar(Key* key, KD_Store* kd, String* val);

  static DataFrame* from_array(Key* key, KD_Store* kd, size_t num, int* array);

  static DataFrame* from_array(Key* key, KD_Store* kd, size_t num, float* array);

  static DataFrame* from_array(Key* key, KD_Store* kd, size_t num, bool* array);

  static DataFrame* from_array(Key* key, KD_Store* kd, size_t num, String** array);

  static DataFrame* from_file(Key* key, KD_Store* kd, char* file_name);
 
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
          column->push_back(&DEFAULT_STRING_VALUE);
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
    size_t index = this->schema_.width();
    switch (col_type) {
      case 'I': {
        IntColumn* coll = col->as_int();
        copy_column = new IntColumn(kv_, name_, index, coll->size_, coll->keys_, coll->buffered_elements_);
        break;
      }
      case 'F': {
        FloatColumn* coll = col->as_float();
        copy_column = new FloatColumn(kv_, name_, index, coll->size_, coll->keys_, coll->buffered_elements_);
        break;
      }
      case 'B': {
        BoolColumn* coll = col->as_bool();
        copy_column = new BoolColumn(kv_, name_, index, coll->size_, coll->keys_, coll->buffered_elements_);
        break;
      }
      case 'S': {
        StringColumn* coll = col->as_string();
        copy_column = new StringColumn(kv_, name_, index, coll->size_, coll->keys_, coll->buffered_elements_);
        break;
      }
      default:
        assert(0);
    }
    return copy_column;
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
        fill_rest_of_column_with_empty_values_(this->cols_->get(ii), col_size - num_rows);
      }
      for (size_t jj = 0; jj < col_size - num_rows; jj++) {
        this->schema_.add_row();
      }
    }

    this->cols_->push(copy_column);
    delete copy_column;
    this->schema_.add_column(col->get_type());
  }

  /** Gets a specific Column inside of the DataFrame. */
  Column* get_column(size_t col) {
    return this->cols_->get(col);
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
  // NOTE: Returns a pointer that can be volatile (if coming from KV store, will overwrite the old
  // cache String pointer, so String address CAN change later), clone if needed longer
  String* get_string(size_t col, size_t row) {
    StringColumn* string_column = get_string_column_(this->cols_, this->schema_.width(), col);
    return string_column->get(row);
  }

  // TODO: Remove all of these set(...) functions as we are dealing with a read_only DataFrame
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
          IntColumn* int_column = this->cols_->get(ii)->as_int();
          int int_value = int_column->get(idx);
          row.set(ii, int_value);
          break;
        }
        case 'F': {
          FloatColumn* float_column = this->cols_->get(ii)->as_float();
          float float_value = float_column->get(idx);
          row.set(ii, float_value);
          break;
        }
        case 'B': {
          BoolColumn* bool_column = this->cols_->get(ii)->as_bool();
          bool bool_value = bool_column->get(idx);
          row.set(ii, bool_value);
          break;
        }
        case 'S': {
          StringColumn* string_column = this->cols_->get(ii)->as_string();
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

      switch (row_type) {
        case 'I': {
          int int_value = row.get_int(ii);
          this->cols_->get(ii)->as_int()->push_back(int_value);
          break;
        }
        case 'F': {
          float float_value = row.get_float(ii);
          this->cols_->get(ii)->as_float()->push_back(float_value);
          break;
        }
        case 'B': {
          bool bool_value = row.get_bool(ii);
          this->cols_->get(ii)->as_bool()->push_back(bool_value);
          break;
        }
        case 'S': {
          String* string_value = row.get_string(ii);
          this->cols_->get(ii)->as_string()->push_back(string_value);
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
 
  // TODO: Remove this as Jan doesn't think we'll need it in the future
  /** Create a new dataframe, constructed from rows for which the given Rower
    * returned true from its accept method. */
  DataFrame* filter(Rower& r, String* name) {
    DataFrame* filtered_dataframe = new DataFrame(*this, name);
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

  // TODO: Maybe remove this since all kv_stores have their own seperate threads anyway
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
      clones[i] = rower.clone();
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
