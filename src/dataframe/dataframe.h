// Made by Kaylin Devchand and Cristian Stransky
#pragma once

#include "../helpers/object.h"
#include "../helpers/string.h"
#include "../kv_store/kv_store.h"
#include "../kv_store/key_array.h"
#include "../helpers/array.h"
#include "column_array.h"
#include "rower.h"
#include "schema.h"

#include <stdarg.h>
#include <assert.h>
#include <cstring>
#include <thread>
 
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
 * Helper function to get an absolutely certain DoubleColumn with error checking. 
 * Throws error if the col_idx is out of bounds, or if the type of the Column is not DoubleColumn.
 */
DoubleColumn* get_double_column_(ColumnArray* columns, size_t columns_size, size_t col_idx) {
  assert(col_idx < columns_size);
  DoubleColumn* double_column = columns->get(col_idx)->as_double();
  assert(double_column);
  return double_column;
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
      case 'D': 
        return new DoubleColumn(kv_, name_, index);
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
      String* name = new String(deserializer);

      DataFrame* new_dataframe = new DataFrame(*schema, name, kv_store, columns);
      delete schema;
      delete columns;
      delete name;
      return new_dataframe;
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
      case 'D': {
        for (size_t jj = 0; jj < num_rows_to_fill; jj++) {
          column->push_back(DEFAULT_DOUBLE_VALUE);
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
      case 'D': {
        DoubleColumn* coll = col->as_double();
        copy_column = new DoubleColumn(kv_, name_, index, coll->size_, coll->keys_, coll->buffered_elements_);
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
  double get_double(size_t col, size_t row) {
    DoubleColumn* double_column = get_double_column_(this->cols_, this->schema_.width(), col);
    return double_column->get(row);
  }
  // NOTE: Returns a pointer that can be volatile (if coming from KV store, will overwrite the old
  // cache String pointer, so String address CAN change later), clone if needed longer
  String* get_string(size_t col, size_t row) {
    StringColumn* string_column = get_string_column_(this->cols_, this->schema_.width(), col);
    return string_column->get(row);
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
        case 'D': {
          DoubleColumn* double_column = this->cols_->get(ii)->as_double();
          double double_value = double_column->get(idx);
          row.set(ii, double_value);
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
        case 'D': {
          double double_value = row.get_double(ii);
          this->cols_->get(ii)->as_double()->push_back(double_value);
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
