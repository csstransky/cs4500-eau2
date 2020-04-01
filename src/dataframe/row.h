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
  size_t idx_;
 
  /** Build a row following a schema. */
  Row(Schema& scm) {
    schema_ = scm;
    size_t width = schema_.width();
    // Array is simply an IntArray to avoid bad deletes. Type handling is now done by the Schema.
    cells_ = new Array('I', width);
    for (size_t i = 0; i < width; i++) 
      switch (schema_.col_type(i)) {
        case 'I': cells_->push_payload(int_to_payload(DEFAULT_INT_VALUE)); break;
        case 'D': cells_->push_payload(double_to_payload(DEFAULT_DOUBLE_VALUE)); break;
        case 'B': cells_->push_payload(bool_to_payload(DEFAULT_BOOL_VALUE)); break;
        case 'S': cells_->push_payload(object_to_payload(DEFAULT_STRING_VALUE.clone())); break;
      }
    // TODO: Didn't Jan say we can delete this? Do we really need index?
    // Default value of idx
    idx_ = SIZE_MAX;
  }

  ~Row() {
    for (size_t ii = 0; ii < schema_.width(); ii++)
      if (schema_.col_type(ii) == 'O')
        delete cells_->get_payload(ii).o;
    delete cells_;
  }
 
  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
  void set(size_t col, int val) { 
    assert(schema_.col_type(col) == 'I');
    // TODO: See if this works later
    // cells_->get_payload(col) = int_to_payload(val);
    cells_->elements_[col].i = val;
  }

  void set(size_t col, double val) {
    assert(schema_.col_type(col) == 'D');
    cells_->get_payload(col) = double_to_payload(val);
  }

  void set(size_t col, bool val) {
    assert(schema_.col_type(col) == 'B');
    cells_->get_payload(col) = bool_to_payload(val);
  }

  /** Acquire ownership of the string. */
  void set(size_t col, String* val) {
    assert(schema_.col_type(col) == 'S');
    delete cells_->get_payload(col).o;
    cells_->get_payload(col) = object_to_payload(val);
  }
 
  /** Set/get the index of this row (ie. its position in the dataframe. This is
   *  only used for informational purposes, unused otherwise */
  void set_idx(size_t idx) { idx_ = idx; }

  /** If never set, returns SIZE_MAX */
  size_t get_idx() { return idx_; }
 
  /** Getters: get the value at the given column. If the column is not
    * of the requested type, the result is undefined. */
  int get_int(size_t col) { 
    assert(schema_.col_type(col) == 'I');
    return cells_->get_payload(col).i; 
  }

  bool get_bool(size_t col) { 
    assert(schema_.col_type(col) == 'B');
    return cells_->get_payload(col).b; 
  }

  double get_double(size_t col) {
    assert(schema_.col_type(col) == 'D');
    return cells_->get_payload(col).d; 
  }
  
  // NOTE: Returns a pointer that can be volatile (if coming from KV store, will overwrite the old
  // cache String pointer, so String address CAN change later), clone if needed longer
  String* get_string(size_t col) {
    assert(schema_.col_type(col) == 'S');
    return static_cast<String*>(cells_->get_payload(col).o);
    
  }
 
  /** Number of fields in the row. */
  size_t width() { return schema_.width(); }
 
   /** Type of the field at the given position. An idx >= width is  undefined. */
  char col_type(size_t idx) { return schema_.col_type(idx); }
 
  /** Given a Fielder, visit every field of this row. The first argument is
    * index of the row in the dataframe.
    * Calling this method before the row's fields have been set is undefined. */
  void visit(size_t idx, Fielder& f) {
    f.start(idx);
    for (size_t i = 0; i < width(); i++)
      switch (col_type(i)) {
        case 'I': f.accept(get_int(i)); break;
        case 'D': f.accept(get_double(i)); break;
        case 'B': f.accept(get_bool(i)); break;
        case 'S': f.accept(get_string(i)); break;
      }
    f.done();
  }
};
 