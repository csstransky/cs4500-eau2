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
      assert(types[ii] == 'I' || types[ii] == 'D' || types[ii] == 'B' || types[ii] == 'S');
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
    assert(typ == 'I' || typ == 'D' || typ == 'B' || typ == 'S');
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