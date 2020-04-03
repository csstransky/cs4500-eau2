#pragma once

#include "../helpers/array.h"
#include "schema.h"

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
  Array* cells_;
  Schema schema_;
 
  /** Build a row following a schema. */
  Row(Schema& scm) : schema_(scm) {
    size_t width = schema_.width();
    // Array is simply an IntArray to avoid bad deletes. Type handling is now done by the Schema.
    cells_ = new Array('I', width);
    for (size_t i = 0; i < width; i++) 
      switch (schema_.col_type(i)) {
        case 'I': cells_->push(int_to_payload(DEFAULT_INT_VALUE)); break;
        case 'D': cells_->push(double_to_payload(DEFAULT_DOUBLE_VALUE)); break;
        case 'B': cells_->push(bool_to_payload(DEFAULT_BOOL_VALUE)); break;
        case 'S': cells_->push(object_to_payload(DEFAULT_STRING_VALUE.clone())); break;
      }
  }

  ~Row() {
    for (size_t ii = 0; ii < schema_.width(); ii++)
      if (schema_.col_type(ii) == 'S')
        delete cells_->get(ii).o;
    delete cells_;
  }
 
  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
  void set(size_t col, int val) { 
    assert(schema_.col_type(col) == 'I');
    cells_->replace(col, int_to_payload(val));
  }

  void set(size_t col, double val) {
    assert(schema_.col_type(col) == 'D');
    cells_->replace(col, double_to_payload(val));
  }

  void set(size_t col, bool val) {
    assert(schema_.col_type(col) == 'B');
    cells_->replace(col, bool_to_payload(val));
  }

  /** Acquire ownership of the string. */
  void set(size_t col, String* val) {
    assert(schema_.col_type(col) == 'S');
    val = val ? val->clone() : DEFAULT_STRING_VALUE.clone();
    Payload p = cells_->replace(col, object_to_payload(val));
    delete p.o;
  }
 
  /** Getters: get the value at the given column. If the column is not
    * of the requested type, the result is undefined. */
  int get_int(size_t col) { 
    assert(schema_.col_type(col) == 'I');
    return cells_->get(col).i; 
  }

  bool get_bool(size_t col) { 
    assert(schema_.col_type(col) == 'B');
    return cells_->get(col).b; 
  }

  double get_double(size_t col) {
    assert(schema_.col_type(col) == 'D');
    return cells_->get(col).d; 
  }
  
  // NOTE: Returns a pointer that can be volatile (if coming from KV store, will overwrite the old
  // cache String pointer, so String address CAN change later), clone if needed longer
  String* get_string(size_t col) {
    assert(schema_.col_type(col) == 'S');
    return static_cast<String*>(cells_->get(col).o);
    
  }
 
  /** Number of fields in the row. */
  size_t width() { return schema_.width(); }
 
   /** Type of the field at the given position. An idx >= width is  undefined. */
  char col_type(size_t idx) { return schema_.col_type(idx); }
};
 