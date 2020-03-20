#pragma once

#include "../array/array.h"
#include "../kv_store/kv_store.h"

// Number of elements each array in the array of arrays in Column have
const int ELEMENT_ARRAY_SIZE = 100;
const int DEFAULT_INT_VALUE = 0;
const float DEFAULT_FLOAT_VALUE = 0;
const bool DEFAULT_BOOL_VALUE = 0;
String DEFAULT_STRING_VALUE("");
const int NUM_THREADS = 4;

class IntColumn;
class FloatColumn;
class BoolColumn;
class StringColumn;
class KD_Store;
class ColumnArray;

size_t max_(size_t a, size_t b) {
  return (a > b) ? a : b;
}

size_t min_(size_t a, size_t b) {
  return (a < b) ? a : b;
}

// TODO: move all Columns to column.h 
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
  KV_Store* kv_; // not owned by Column, simply used for kv methods
  String* dataframe_name_; // not owned by Column
  size_t index_;
  KeyArray* keys_; // owned
 
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

  size_t get_num_arrays() {
    return size_ / ELEMENT_ARRAY_SIZE + 1;
  }

  void set_parent_values_(size_t size, char type, KV_Store* kv, String* dataframe_name, 
    size_t col_index) {
    kv_ = kv;
    dataframe_name_ = dataframe_name ? dataframe_name->clone() : nullptr; // TODO: but this is not owned by the df. Does it need to be?
    index_ = col_index;
    type_ = type;
    size_ = size;
    size_t num_arrays_ = get_num_arrays();
    keys_ = new KeyArray(num_arrays_);
  }

  void set_parent_values_(size_t size, char type, KV_Store* kv, String* dataframe_name, 
    size_t col_index, KeyArray* keys) { 
    kv_ = kv;
    dataframe_name_ = dataframe_name ? dataframe_name->clone() : nullptr; // TODO: but this is not owned by the df. Does it need to be?
    index_ = col_index;
    type_ = type;
    size_ = size;
    keys_ = keys->clone();
  }

  Key* generate_key_(size_t array_index) {
    String key_name(*dataframe_name_);
    key_name.concat("_");
    key_name.concat(index_);
    key_name.concat("_");
    key_name.concat(array_index);

    Key* new_key = new Key(&key_name, kv_->get_node_index());
    return new_key;    
  }
 
 /** Returns the number of elements in the column. */
  virtual size_t size() {
    return size_;
  }
 
  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
  char get_type() {
    return type_;
  }

  size_t column_serial_size_(Array* buffered_elements) {
    return sizeof(size_t) // serial_size
      + sizeof(char) // type_
      + sizeof(size_t) // size_
      + dataframe_name_->serial_len()
      + sizeof(index_) // index_
      + keys_->serial_len()
      + buffered_elements->serial_len();
  }

  void serialize_column_(Serializer& serializer, Array* buffered_elements) {
    serializer.serialize_size_t(serializer.get_serial_size());
    serializer.serialize_char(type_);
    serializer.serialize_size_t(size_);
    serializer.serialize_object(dataframe_name_);
    serializer.serialize_size_t(index_);
    serializer.serialize_object(keys_);
    serializer.serialize_object(buffered_elements);
  }

  static Column* deserialize(char* serial, KV_Store* kv_store) {
    Deserializer deserializer(serial);
    return deserialize(deserializer, kv_store);
  }

  static char get_column_type(Deserializer& deserializer) {
    size_t deserial_start_index = deserializer.get_serial_index();
    deserializer.deserialize_size_t(); // skip serial_length
    char column_type = deserializer.deserialize_char();
    deserializer.set_serial_index(deserial_start_index); // reset serial index back to the beginning
    return column_type;
  }

  static Column* deserialize(Deserializer& deserializer, KV_Store* kv_store);
};
 
/*************************************************************************
 * IntColumn::
 * Holds int values.
 */
class IntColumn : public Column {
 public:

  // Elements are stored as an array of int arrays where each int array holds ELEMENT_ARRAY_SIZE 
  // number of elements.
  IntArray* buffered_elements_;  

  IntColumn(KV_Store* kv, String* name, size_t index, size_t size, KeyArray* keys, 
    IntArray* local_array) {
    set_parent_values_(size, 'I', kv, name, index, keys);
    buffered_elements_ = local_array->clone();
  }

  IntColumn(KV_Store* kv, String* name, size_t index) {
    set_parent_values_(0, 'I', kv, name, index);
    buffered_elements_ = new IntArray(ELEMENT_ARRAY_SIZE);
  }

  IntColumn(IntColumn& other) 
    : IntColumn(other.kv_, other.dataframe_name_, other.index_, other.size_, other.keys_, 
      other.buffered_elements_) {

  }

  IntColumn() : IntColumn(nullptr, nullptr, 0) {

  }

  ~IntColumn() {
    // TODO: may need to watch out for nullptr in the future
    delete dataframe_name_;
    delete buffered_elements_;
    delete keys_;
  }

  IntColumn* clone() {
    return new IntColumn(*this);
  }

  int get(size_t idx) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;
    size_t last_array = size_ / ELEMENT_ARRAY_SIZE;

    // if element is in last array, get buffered_elements_ ow get blocks
    if (last_array == array) {
      return buffered_elements_->get(index);
    } else {
      Key* k = keys_->get(array);
      IntArray* data = kv_->get_int_array(k);
      int i = data->get(index);
      delete data;
      return i;
    }
  }

  IntColumn* as_int() { return this; }


  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, int val) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;
    size_t last_array = size_ / ELEMENT_ARRAY_SIZE;

    // if element is to be place in last array, update buffered_elements, else get from kvstore
    if (last_array == array) {
      buffered_elements_->replace(index, val);
    } else {
      Key* k = keys_->get(array);
      IntArray* data = kv_->get_int_array(k);
      data->replace(index, val);
      kv_->put(k, data);
      delete data;
    }

  }

  size_t size() {
    // Parent provides all the implementation needed.
    return Column::size();
  }

  void push_back(int val) {
    size_t array = size_ / ELEMENT_ARRAY_SIZE;
    size_t index = size_ % ELEMENT_ARRAY_SIZE;

    buffered_elements_->push(val);
    
    // If buffered elements is full, create key and add to kvstore
    if (index == ELEMENT_ARRAY_SIZE - 1) {
      Key* k = generate_key_(array);
      keys_->push(k);
      kv_->put(k, buffered_elements_); 
      delete k;
      delete buffered_elements_;
      buffered_elements_ = new IntArray(ELEMENT_ARRAY_SIZE);
    }

    size_++;
  }

  size_t serial_len() {
    return column_serial_size_(buffered_elements_);
  }

  char* serialize() {
    size_t serial_size = serial_len();
    Serializer serializer(serial_size);
    serialize_column_(serializer, buffered_elements_);
    return serializer.get_serial();
  }

  static IntColumn* deserialize(char* serial, KV_Store* kv_store) {
    Deserializer deserializer(serial);
    return deserialize(deserializer, kv_store);
  }

  static IntColumn* deserialize(Deserializer& deserializer, KV_Store* kv_store) {
    deserializer.deserialize_size_t(); // skip serial size
    deserializer.deserialize_char(); // skip type
    size_t dataframe_size = deserializer.deserialize_size_t(); 
    String* dataframe_name = String::deserialize(deserializer);
    size_t dataframe_index = deserializer.deserialize_size_t();
    KeyArray* dataframe_keys = KeyArray::deserialize(deserializer);
    IntArray* buffered_elements = IntArray::deserialize(deserializer);

    IntColumn* new_int_column = new IntColumn(kv_store, dataframe_name, dataframe_index, 
      dataframe_size, dataframe_keys, buffered_elements);
    delete dataframe_name;
    delete dataframe_keys;
    delete buffered_elements;
    return new_int_column;
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
  FloatArray* buffered_elements_;

  FloatColumn(KV_Store* kv, String* name, size_t index, size_t size, KeyArray* keys, 
    FloatArray* local_array) {
    set_parent_values_(size, 'F', kv, name, index, keys);
    buffered_elements_ = local_array->clone();
  }

  FloatColumn(KV_Store* kv, String* name, size_t index) {
    set_parent_values_(0, 'F', kv, name, index);
    buffered_elements_ = new FloatArray(ELEMENT_ARRAY_SIZE);
  }

  FloatColumn(FloatColumn& other) 
    : FloatColumn(other.kv_, other.dataframe_name_, other.index_, other.size_, other.keys_, 
      other.buffered_elements_) {

  }

  FloatColumn() : FloatColumn(nullptr, nullptr, 0) {
    
  }

  ~FloatColumn() {
    delete dataframe_name_;
    delete buffered_elements_;
    delete keys_;
  }

  FloatColumn* clone() {
    return new FloatColumn(*this);
  }

  float get(size_t idx) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;
    size_t last_array = size_ / ELEMENT_ARRAY_SIZE;

    // if element is in last array, get buffered_elements_ ow get blocks
    if (last_array == array) {
      return buffered_elements_->get(index);
    } else {
      Key* k = keys_->get(array);
      FloatArray* data = kv_->get_float_array(k);
      float i = data->get(index);
      delete data;
      return i;
    }
  }

  FloatColumn* as_float() { return this; }


  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, float val) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;
    size_t last_array = size_ / ELEMENT_ARRAY_SIZE;

    // if element is to be place in last array, update buffered_elements, else get from kvstore
    if (last_array == array) {
      buffered_elements_->replace(index, val);
    } else {
      Key* k = keys_->get(array);
      FloatArray* data = kv_->get_float_array(k);
      data->replace(index, val);
      kv_->put(k, data);
      delete data;
    }
  }

  size_t size() {
    // Parent provides all the implementation needed.
    return Column::size();
  }

  void push_back(float val) {
    size_t array = size_ / ELEMENT_ARRAY_SIZE;
    size_t index = size_ % ELEMENT_ARRAY_SIZE;

    buffered_elements_->push(val);
    
    // If buffered elements is full, create key and add to kvstore
    if (index == ELEMENT_ARRAY_SIZE - 1) {
      Key* k = generate_key_(array);
      keys_->push(k);
      kv_->put(k, buffered_elements_); 
      delete k;
      delete buffered_elements_;
      buffered_elements_ = new FloatArray(ELEMENT_ARRAY_SIZE);
    }

    size_++;
  }

  size_t serial_len() {
    return column_serial_size_(buffered_elements_);
  }

  char* serialize() {
    size_t serial_size = serial_len();
    Serializer serializer(serial_size);
    serialize_column_(serializer, buffered_elements_);
    return serializer.get_serial();
  }

  static FloatColumn* deserialize(char* serial, KV_Store* kv_store) {
    Deserializer deserializer(serial);
    return deserialize(deserializer, kv_store);
  }

  static FloatColumn* deserialize(Deserializer& deserializer, KV_Store* kv_store) {
    deserializer.deserialize_size_t(); // skip serial size
    deserializer.deserialize_char(); // skip type
    size_t dataframe_size = deserializer.deserialize_size_t(); 
    String* dataframe_name = String::deserialize(deserializer);
    size_t dataframe_index = deserializer.deserialize_size_t();
    KeyArray* dataframe_keys = KeyArray::deserialize(deserializer);
    FloatArray* buffered_elements = FloatArray::deserialize(deserializer);

    FloatColumn* new_float_column = new FloatColumn(kv_store, dataframe_name, dataframe_index, 
      dataframe_size, dataframe_keys, buffered_elements);
    delete dataframe_name;
    delete dataframe_keys;
    delete buffered_elements;
    return new_float_column;
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
  BoolArray* buffered_elements_;

  BoolColumn(KV_Store* kv, String* name, size_t index, size_t size, KeyArray* keys, 
    BoolArray* local_array) {
    set_parent_values_(size, 'B', kv, name, index, keys);
    buffered_elements_ = local_array->clone();
  }

  BoolColumn(KV_Store* kv, String* name, size_t index) {
    set_parent_values_(0, 'B', kv, name, index);
    buffered_elements_ = new BoolArray(ELEMENT_ARRAY_SIZE);
  }

  BoolColumn(BoolColumn& other) 
    : BoolColumn(other.kv_, other.dataframe_name_, other.index_, other.size_, other.keys_, 
      other.buffered_elements_) {

  }

  BoolColumn() : BoolColumn(nullptr, nullptr, 0) {
    
  }

  BoolColumn* clone() {
    return new BoolColumn(*this);
  }

  ~BoolColumn() {
    delete dataframe_name_;
    delete buffered_elements_;
    delete keys_;
  }

  bool get(size_t idx) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;
    size_t last_array = size_ / ELEMENT_ARRAY_SIZE;

    // if element is in last array, get buffered_elements_ ow get blocks
    if (last_array == array) {
      return buffered_elements_->get(index);
    } else {
      Key* k = keys_->get(array);
      BoolArray* data = kv_->get_bool_array(k);
      bool i = data->get(index);
      delete data;
      return i;
    }
  }

  BoolColumn* as_bool() { return this; }


  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, bool val) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;
    size_t last_array = size_ / ELEMENT_ARRAY_SIZE;

    // if element is to be place in last array, update buffered_elements, else get from kvstore
    if (last_array == array) {
      buffered_elements_->replace(index, val);
    } else {
      Key* k = keys_->get(array);
      BoolArray* data = kv_->get_bool_array(k);
      data->replace(index, val);
      kv_->put(k, data);
      delete data;
    }
  }

  size_t size() {
    // Parent provides all the implementation needed.
    return Column::size();
  }

  void push_back(bool val) {
    size_t array = size_ / ELEMENT_ARRAY_SIZE;
    size_t index = size_ % ELEMENT_ARRAY_SIZE;

    buffered_elements_->push(val);
    
    // If buffered elements is full, create key and add to kvstore
    if (index == ELEMENT_ARRAY_SIZE - 1) {
      Key* k = generate_key_(array);
      keys_->push(k);
      kv_->put(k, buffered_elements_); 
      delete k;
      delete buffered_elements_;
      buffered_elements_ = new BoolArray(ELEMENT_ARRAY_SIZE);
    }

    size_++;
  }

  size_t serial_len() {
    return column_serial_size_(buffered_elements_);
  }

  char* serialize() {
    size_t serial_size = serial_len();
    Serializer serializer(serial_size);
    serialize_column_(serializer, buffered_elements_);
    return serializer.get_serial();
  }

  static BoolColumn* deserialize(char* serial, KV_Store* kv_store) {
    Deserializer deserializer(serial);
    return deserialize(deserializer, kv_store);
  }

  static BoolColumn* deserialize(Deserializer& deserializer, KV_Store* kv_store) {
    deserializer.deserialize_size_t(); // skip serial size
    deserializer.deserialize_char(); // skip type
    size_t dataframe_size = deserializer.deserialize_size_t(); 
    String* dataframe_name = String::deserialize(deserializer);
    size_t dataframe_index = deserializer.deserialize_size_t();
    KeyArray* dataframe_keys = KeyArray::deserialize(deserializer);
    BoolArray* buffered_elements = BoolArray::deserialize(deserializer);

    BoolColumn* new_bool_column = new BoolColumn(kv_store, dataframe_name, dataframe_index, 
      dataframe_size, dataframe_keys, buffered_elements);
    delete dataframe_name;
    delete dataframe_keys;
    delete buffered_elements;
    return new_bool_column;
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
  StringArray* buffered_elements_;

  StringColumn(KV_Store* kv, String* name, size_t index, size_t size, KeyArray* keys, 
    StringArray* local_array) {
    set_parent_values_(size, 'S', kv, name, index, keys);
    buffered_elements_ = local_array->clone();
  }

  StringColumn(KV_Store* kv, String* name, size_t index) {
    set_parent_values_(0, 'S', kv, name, index);
    buffered_elements_ = new StringArray(ELEMENT_ARRAY_SIZE);
  }

  StringColumn(StringColumn& other) 
    : StringColumn(other.kv_, other.dataframe_name_, other.index_, other.size_, other.keys_, 
      other.buffered_elements_) {

  }

  StringColumn() : StringColumn(nullptr, nullptr, 0) {
    
  }

  ~StringColumn() {
    delete dataframe_name_;
    delete buffered_elements_;
    delete keys_;
  }

  StringColumn* clone() {
    return new StringColumn(*this);
  }

  // NOTE: Returns a clone, make sure to delete afterward
  String* get(size_t idx) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;
    size_t last_array = size_ / ELEMENT_ARRAY_SIZE;

    String* s;

    // if element is in last array, get buffered_elements_ ow get blocks
    if (last_array == array) {
      s = buffered_elements_->get(index)->clone();
    } else {
      Key* k = keys_->get(array);
      StringArray* data = kv_->get_string_array(k);
      s = data->get(index)->clone();
      delete data;
    }
    return s;
  }

  StringColumn* as_string() { return this; }


  /** Set value at idx. An out of bound idx is undefined.  */
  void set(size_t idx, String* val) {
    assert(idx < size_);
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    size_t index = idx % ELEMENT_ARRAY_SIZE;
    size_t last_array = size_ / ELEMENT_ARRAY_SIZE;

    // if element is to be place in last array, update buffered_elements, else get from kvstore
    if (last_array == array) {
      String* old = buffered_elements_->replace(index, val);
      delete old;
    } else {
      Key* k = keys_->get(array);
      StringArray* data = kv_->get_string_array(k);
      String* old = data->replace(index, val);
      delete old;
      kv_->put(k, data);
      delete data;
    }
  }

  size_t size() {
    // Parent provides all the implementation needed.
    return Column::size();
  }

  void push_back(String* val) {
    if (val == nullptr) {
      val = &DEFAULT_STRING_VALUE;
    }
    size_t array = size_ / ELEMENT_ARRAY_SIZE;
    size_t index = size_ % ELEMENT_ARRAY_SIZE;

    buffered_elements_->push(val);
    
    // If buffered elements is full, create key and add to kvstore
    if (index == ELEMENT_ARRAY_SIZE - 1) {
      Key* k = generate_key_(array);
      keys_->push(k);
      kv_->put(k, buffered_elements_); 
      delete k;
      delete buffered_elements_;
      buffered_elements_ = new StringArray(ELEMENT_ARRAY_SIZE);
    }

    size_++;

  }
// 
  size_t serial_len() {
    return column_serial_size_(buffered_elements_);
  }

  char* serialize() {
    size_t serial_size = serial_len();
    Serializer serializer(serial_size);
    serialize_column_(serializer, buffered_elements_);
    return serializer.get_serial();
  }

  static StringColumn* deserialize(char* serial, KV_Store* kv_store) {
    Deserializer deserializer(serial);
    return deserialize(deserializer, kv_store);
  }

  static StringColumn* deserialize(Deserializer& deserializer, KV_Store* kv_store) {
    deserializer.deserialize_size_t(); // skip serial size
    deserializer.deserialize_char(); // skip type
    size_t dataframe_size = deserializer.deserialize_size_t(); 
    String* dataframe_name = String::deserialize(deserializer);
    size_t dataframe_index = deserializer.deserialize_size_t();
    KeyArray* dataframe_keys = KeyArray::deserialize(deserializer);
    StringArray* buffered_elements = StringArray::deserialize(deserializer);

    StringColumn* new_string_column = new StringColumn(kv_store, dataframe_name, dataframe_index, 
      dataframe_size, dataframe_keys, buffered_elements);
    delete dataframe_name;
    delete dataframe_keys;
    delete buffered_elements;
    return new_string_column;
  }
};

Column* Column::deserialize(Deserializer& deserializer, KV_Store* kv_store) {
  char column_type = get_column_type(deserializer);
  switch(column_type) {
    case 'I':
      return IntColumn::deserialize(deserializer, kv_store);
    case 'F':
      return FloatColumn::deserialize(deserializer, kv_store);
    case 'B':
      return BoolColumn::deserialize(deserializer, kv_store);
    case 'S':
      return StringColumn::deserialize(deserializer, kv_store);
    default:
      assert(0);
  }
}