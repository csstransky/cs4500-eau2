#include "column_array.h"
#include "dataframe.h"

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
        case 'D': {
          DoubleColumn temp_column;
          cols_->push(&temp_column);
          cols_->get(i)->push_back(DEFAULT_DOUBLE_VALUE);
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

  void set(size_t col, double val) {
    DoubleColumn* column = get_double_column_(this->cols_, this->width_, col);
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

  double get_double(size_t col) {
    DoubleColumn* column = get_double_column_(this->cols_, this->width_, col);
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
        case 'D':
          f.accept(get_double(i));
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
 