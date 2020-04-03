#pragma once

#include "../helpers/array.h"
#include "../kv_store/kv_store.h"
#include "../kv_store/key_array.h"

// Number of elements each array in the array of arrays in Column have
// TODO: Now we know a 10GB is our average file size, we should mess with ELEMENT_ARRAY_SIZE until
// we get a number that's optimal. Jan does say 100 is too small though.
const int ELEMENT_ARRAY_SIZE = 100; 
const int DEFAULT_INT_VALUE = 0;
const double DEFAULT_DOUBLE_VALUE = 0;
const bool DEFAULT_BOOL_VALUE = 0;
String DEFAULT_STRING_VALUE("");
const int NUM_THREADS = 4;

class KD_Store;
class ColumnArray;

/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. 
 * Authors: Kaylin Devchand & Cristian Stransky
 * */
class Column : public Object {
 public:

  char type_;
  size_t size_;
  KV_Store* kv_; // not owned by Column, simply used for kv methods
  String* dataframe_name_;
  size_t column_index_;
  KeyArray* keys_; // owned
  int node_for_chunk_; // TODO: remove this field in the future
  // Elements are stored as an array of int arrays where each int array holds ELEMENT_ARRAY_SIZE 
  // number of elements.
  Array* buffered_elements_;  
  // Since Strings can be stored in a kv_store, we need to be able to use get(...) without having to 
  // worry about deleting the deserialized String later, this cache string will be used to hold it
  String* cache_string_; // TODO: This will change in the future when we add a real cache

  Column(char type, KV_Store* kv, String* dataframe_name, size_t index, size_t size, KeyArray* keys, 
    Array* local_array) {
    type_ = type;
    size_ = size;
    kv_ = kv;
    dataframe_name_ = dataframe_name? dataframe_name->clone() : nullptr;
    column_index_ = index;
    keys_ = keys ? keys->clone() : nullptr;
    node_for_chunk_ = 0; // TODO: remove this field in the future
    buffered_elements_ = local_array ? local_array->clone() : nullptr;
    cache_string_ = nullptr;
  }

  // TODO: I think I'm gonna have to valgrind this and fix it later
  Column(char type, KV_Store* kv, String* name, size_t index) 
    : Column(type, kv, name, index, 0, nullptr, nullptr) { 
      keys_ = new KeyArray(1);
      switch(type) {
        case 'I': buffered_elements_ = new IntArray(ELEMENT_ARRAY_SIZE); break;
        case 'B': buffered_elements_ = new BoolArray(ELEMENT_ARRAY_SIZE); break;
        case 'D': buffered_elements_ = new DoubleArray(ELEMENT_ARRAY_SIZE); break;
        case 'S': buffered_elements_ = new StringArray(ELEMENT_ARRAY_SIZE); break;
      }
      
    }

  Column(Column& other) 
    : Column(other.type_, other.kv_, other.dataframe_name_, other.column_index_, other.size_, 
      other.keys_, other.buffered_elements_) { }
  
  Column(Column& other, String* df_name, KV_Store* kv, size_t index) 
    : Column(other.type_, kv, df_name, index, other.size_, 
      other.keys_, other.buffered_elements_) { }

  Column(char type) : Column(type, nullptr, nullptr, 0) {  }

  Column(Deserializer& deserializer, KV_Store* kv_store) {
    kv_ = kv_store;
    type_ = deserializer.deserialize_char();
    size_ = deserializer.deserialize_size_t(); 
    dataframe_name_ = new String(deserializer);
    column_index_ = deserializer.deserialize_size_t();
    keys_ = new KeyArray(deserializer);
    switch(type_) {
      case 'I': buffered_elements_ = new IntArray(deserializer); break;
      case 'B': buffered_elements_ = new BoolArray(deserializer); break;
      case 'D': buffered_elements_ = new DoubleArray(deserializer); break;
      case 'S': buffered_elements_ = new StringArray(deserializer); break;
    }
    cache_string_ = nullptr;
  }

  ~Column() {
    delete dataframe_name_;
    delete buffered_elements_;
    delete keys_;
    delete cache_string_;
  }

  Column* clone() { return new Column(*this); }

  Key* generate_key_(size_t array_index) {
    String key_name(*dataframe_name_);
    key_name.concat("_");
    key_name.concat(column_index_);
    key_name.concat("_");
    key_name.concat(array_index);

    size_t home_index = kv_->get_node_index(node_for_chunk_++ % kv_->get_num_other_nodes());
    Key* new_key = new Key(&key_name, home_index);
    return new_key;    
  }

  bool is_buffer_full_() { return size_ % ELEMENT_ARRAY_SIZE == ELEMENT_ARRAY_SIZE - 1; }

  void store_buffer_in_kv_() {
    size_t key_index = size_ / ELEMENT_ARRAY_SIZE;
    Key* k = generate_key_(key_index);
    keys_->push(k);
    kv_->put(k, buffered_elements_); 
    delete k;
    buffered_elements_->clear();
  }

  void push_back_payload_(Payload payload) {
    buffered_elements_->push(payload);
    if (is_buffer_full_()) store_buffer_in_kv_();
    size_++;
  }
 
  /** Type appropriate push_back methods. Calling the wrong method is
    * undefined behavior. **/
  void push_back(int val) {
    assert(type_ == 'I');
    push_back_payload_(int_to_payload(val));
  }

  void push_back(bool val) {
    assert(type_ == 'B');
    push_back_payload_(bool_to_payload(val));
  }

  void push_back(double val) {
    assert(type_ == 'D');
    push_back_payload_(double_to_payload(val));
  }

  void push_back(String* val) {
    assert(type_ == 'S');
    String* string_clone = val ? val->clone() : DEFAULT_STRING_VALUE.clone();
    push_back_payload_(object_to_payload(string_clone));
  }

  // TODO: Change this to a cache that uses both get and put correctly in the future
  Payload get_cached_element_(size_t idx) {
    size_t index = idx % ELEMENT_ARRAY_SIZE;
    return buffered_elements_->get(index);
  }

  Payload get_kv_stored_element_(size_t idx) {
    size_t index = idx % ELEMENT_ARRAY_SIZE;
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    Key* k = keys_->get(array);
    Array* data = kv_->get_array(k, type_);
    Payload payload = data->get(index);
    if (type_ == 'S') {
      payload.o = payload.o->clone();
      delete cache_string_;
      cache_string_ = static_cast<String*>(payload.o);
    }
    delete data;
    return payload;
  }

  bool is_index_in_cache_(size_t idx) { 
    return size_ / ELEMENT_ARRAY_SIZE == idx / ELEMENT_ARRAY_SIZE;
  }

  Payload get_element_(size_t idx) {
    assert(idx < size_);
    if (is_index_in_cache_(idx))
      return get_cached_element_(idx);
    else {
      return get_kv_stored_element_(idx);
    }
  }

  int get_int(size_t idx) {
    assert(type_ == 'I');
    return get_element_(idx).i;
  }

  bool get_bool(size_t idx) {
    assert(type_ == 'B');
    return get_element_(idx).b;
  }

  double get_double(size_t idx) {
    assert(type_ == 'D');
    return get_element_(idx).d;
  }

  String* get_string(size_t idx) {
    assert(type_ == 'S');
    return static_cast<String*>(get_element_(idx).o);
  }
 
  /** Returns the number of elements in the column. */
  size_t size() {
    return size_;
  }
 
  /** Return the type of this column as a char: 'S', 'B', 'I' and 'D'. */
  char get_type() {
    return type_;
  }

  size_t serial_len() {
    return sizeof(char) // type_
      + sizeof(size_t) // size_
      + dataframe_name_->serial_len()
      + sizeof(column_index_) // column_index_
      + keys_->serial_len()
      + buffered_elements_->serial_len();
  }

  char* serialize() {
    size_t serial_size = serial_len();
    Serializer serializer(serial_size);
    serializer.serialize_char(type_);
    serializer.serialize_size_t(size_);
    serializer.serialize_object(dataframe_name_);
    serializer.serialize_size_t(column_index_);
    serializer.serialize_object(keys_);
    serializer.serialize_object(buffered_elements_);
    return serializer.get_serial();
  }
};