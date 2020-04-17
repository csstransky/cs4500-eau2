// Made by Kaylin Devchand and Cristian Stransky
#pragma once

#include "../helpers/object.h"
#include "../helpers/string.h"
#include "../kv_store/kv_store.h"
#include "../kv_store/key_array.h"
#include "../helpers/array.h"
#include "column_array.h"
#include "rower.h"
#include "row.h"
#include "schema.h"

#include <stdarg.h>
#include <assert.h>
#include <cstring>
#include <thread>

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
  KV_Store* kv_; // not owned

  /** Create a data frame from a schema and columns. All columns are created
    * empty. */
  DataFrame(Schema& schema, KV_Store* kv) {
    kv_ = kv;
    size_t num_cols = schema.width();
    // It's possible to make a schema with 0 columns, so we want to make sure we have atleast an 
    // array size of 1, or else our doubling in size arthimetic won't work correctly
    size_t col_size = max(num_cols, 1);
    this->cols_ = new ColumnArray(col_size);
    
    for (size_t ii = 0; ii < num_cols; ii++) {
        char col_type = schema.col_type(ii);
        this->schema_.add_column(col_type);

        Column temp_col(col_type, kv_);
        this->cols_->push(&temp_col);
    }
  }

  /** copy constructor mainly used for deserialization */
  DataFrame(Schema& schema, KV_Store* kv, ColumnArray* columns) : schema_(schema) {
    this->kv_ = kv;
    this->cols_ = columns->clone();    
  }

  /** Create a data frame with the same columns as the given df but with no rows or rownmaes */
  DataFrame(DataFrame& df) : DataFrame(df.get_schema(), df.kv_) { }

  DataFrame(Deserializer& deserializer, KV_Store* kv_store) : schema_(deserializer) {
    cols_ = new ColumnArray(deserializer, kv_store);
    kv_ = kv_store;
  }
  
  ~DataFrame() {
    delete cols_;
  }

  /** Subclasses should redefine */
  bool equals(Object* other) {
    DataFrame* other_df = dynamic_cast<DataFrame*>(other);
    return other_df != nullptr
      && schema_.equals(&other_df->schema_)
      && cols_->equals(other_df->cols_);
  }

  /** Return a copy of the object; nullptr is considered an error */
  DataFrame* clone() {
    return new DataFrame(schema_, kv_, cols_);
  }

  size_t serial_len() {
    return schema_.serial_len()
      + cols_->serial_len();
  }

  char* serialize() {
      size_t serial_size = serial_len();
      Serializer serializer(serial_size);
      serializer.serialize_object(&schema_);
      serializer.serialize_object(cols_);
      return serializer.get_serial();
  }

  // Implemented in kd_store.h to remove circular dependency. See piazza post @963
  static DataFrame* from_scalar(Key* key, KD_Store* kd, int val);
  static DataFrame* from_scalar(Key* key, KD_Store* kd, double val);
  static DataFrame* from_scalar(Key* key, KD_Store* kd, bool val);
  static DataFrame* from_scalar(Key* key, KD_Store* kd, String* val);
  static DataFrame* from_array(Key* key, KD_Store* kd, size_t num, int* array);
  static DataFrame* from_array(Key* key, KD_Store* kd, size_t num, double* array);
  static DataFrame* from_array(Key* key, KD_Store* kd, size_t num, bool* array);
  static DataFrame* from_array(Key* key, KD_Store* kd, size_t num, String** array);
  static DataFrame* from_file(Key* key, KD_Store* kd, char* file_name);
  static DataFrame* from_rower(Key* key, KD_Store* kd, const char* schema, Rower& rower);
 
  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema& get_schema() { return this->schema_; }

  /** Gets a specific Column inside of the DataFrame. */
  Column* get_column(size_t col) { return this->cols_->get(col); }
 
  /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
  int get_int(size_t col, size_t row) { return this->cols_->get(col)->get_int(row); }

  bool get_bool(size_t col, size_t row) { return cols_->get(col)->get_bool(row); }

  double get_double(size_t col, size_t row) { return cols_->get(col)->get_double(row); }

  // NOTE: Returns a pointer that can be volatile (if coming from KV store, will overwrite the old
  // cache String pointer, so String address CAN change later), clone if needed longer
  String* get_string(size_t col, size_t row) { return cols_->get(col)->get_string(row); }

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
        case 'I': row.set(ii, get_int(ii, idx)); break;
        case 'D': row.set(ii, get_double(ii, idx)); break;
        case 'B': row.set(ii, get_bool(ii, idx)); break;
        case 'S': row.set(ii, get_string(ii, idx)); break;
      } 
    }
  }
  
  size_t nrows() { return this->schema_.length(); }
 
  size_t ncols() { return this->schema_.width(); }
 
  /** Visit rows in order */
  void map(Rower& r) {
    size_t num_rows = this->schema_.length();
    Row* row = new Row(this->schema_);
    for (size_t ii = 0; ii < num_rows; ii++) {
      this->fill_row(ii, *row);
      r.accept(*row);
    }
    delete row;
  }

  void local_map(Rower& r) {
    size_t num_rows = this->schema_.length();
    Row* row = new Row(this->schema_);
    for (size_t ii = 0; ii < num_rows; ii++) {
      if (kv_->get_node_index() != cols_->get(0)->get_home_node(ii)) continue;
      this->fill_row(ii, *row);
      r.accept(*row);
    }
    delete row;
  }
};
