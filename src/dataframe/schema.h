#pragma once

#include "../helpers/string.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'D'.
 * Authors: Kaylin Devchand & Cristian Stransky
 */
class Schema : public Object {
 public:
  String* types_;
  size_t num_cols_;
  size_t num_rows_; 
 
  /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr argument is
    * undefined. **/
  Schema(const char* types) {
    assert(types != nullptr);
    size_t types_length = strlen(types);
    for (size_t ii = 0; ii < types_length; ii++)
      assert(types[ii] == 'I' || types[ii] == 'D' || types[ii] == 'B' || types[ii] == 'S');

    types_ = new String(types, types_length);
    num_cols_ = types_length; 
    num_rows_ = 0;
  }

  Schema() : Schema("") { }

  Schema(Schema& from) { 
    this->types_ = from.types_->clone();
    this->num_cols_ = from.num_cols_;
    this->num_rows_ = from.num_rows_; 
  }

  Schema(char* serial) {
    Deserializer deserializer(serial);
    deserialize_schema_(deserializer);
  }

  Schema(Deserializer& deserializer) {
    deserialize_schema_(deserializer);
  }

  void deserialize_schema_(Deserializer& deserializer) {
    deserializer.deserialize_size_t(); // skip serial_length
    num_cols_ = deserializer.deserialize_size_t();
    num_rows_ = deserializer.deserialize_size_t();
    types_ = new String(deserializer); 
  }  

  ~Schema() {
    delete types_;
  }

  /** Subclasses should redefine */
  bool equals(Object* other) {
      if (other == this) return true;
      Schema* other_schema = dynamic_cast<Schema*>(other);
      return other_schema != nullptr 
          && this->num_cols_ == other_schema->num_cols_
          && this->num_rows_ == other_schema->num_rows_
          && this->types_->equals(other_schema->types_);
  }

  /** Return a copy of the object; nullptr is considered an error */
  Schema* clone() { return new Schema(*this); }

  size_t serial_len() {
      return sizeof(size_t) // serial_length
        + sizeof(size_t) // num_cols_
        + sizeof(size_t) // num_rows_
        + types_->serial_len();
  }

  char* serialize() {
      size_t serial_size = serial_len();
      Serializer serializer(serial_size);
      serializer.serialize_size_t(serial_size);
      serializer.serialize_size_t(num_cols_);
      serializer.serialize_size_t(num_rows_);
      serializer.serialize_object(types_);
      return serializer.get_serial();
  }

  /** Add a column of the given type and name (can be nullptr), name
    * is external. Names are expectd to be unique, duplicates result
    * in undefined behavior. */
  void add_column(char typ) {
    assert(typ == 'I' || typ == 'D' || typ == 'B' || typ == 'S');
    this->types_->concat(typ);
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
    return this->types_->cstr_[idx];
  }
 
  size_t width() { return num_cols_; }
 
  size_t length() { return num_rows_; }
};