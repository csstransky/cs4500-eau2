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
  KeyArray* keys_; // owned
  String* cache_string_; // TODO: This will change in the future when we add a real cache

  Column(char type, KV_Store* kv, size_t size, KeyArray* keys) {
    type_ = type;
    size_ = size;
    kv_ = kv;
    keys_ = keys ? keys->clone() : nullptr;
    cache_string_ = nullptr;
  }

  Column(char type, KV_Store* kv) : Column(type, kv, 0, nullptr) { 
      keys_ = new KeyArray(1);  
  }

  Column(Column& other) : Column(other.type_, other.kv_, other.size_, other.keys_) { }
  
  Column(Column& other, KV_Store* kv) : Column(other.type_, kv, other.size_, other.keys_) { }

  Column(char type) : Column(type, nullptr) {  }

  Column(Deserializer& deserializer, KV_Store* kv_store) {
    kv_ = kv_store;
    type_ = deserializer.deserialize_char();
    size_ = deserializer.deserialize_size_t(); 
    keys_ = new KeyArray(deserializer);
    cache_string_ = nullptr;
  }

  ~Column() {
    delete keys_;
    delete cache_string_;
  }

  Column* clone() { return new Column(*this); }

  /** Type appropriate push_back methods. Calling the wrong method is
    * undefined behavior. **/
  void push_back(Array* val, Key* key) {
    keys_->push(key);
    kv_->put(key, val); 
    size_ += val->length();
  }

  Payload get_element_(size_t idx) {
    assert(idx < size_);
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

  size_t get_home_node(size_t idx) {
    size_t array = idx / ELEMENT_ARRAY_SIZE;
    return keys_->get(array)->get_node_index();
  }

  size_t serial_len() {
    return sizeof(char) // type_
      + sizeof(size_t) // size_
      + keys_->serial_len();
  }

  char* serialize() {
    size_t serial_size = serial_len();
    Serializer serializer(serial_size);
    serializer.serialize_char(type_);
    serializer.serialize_size_t(size_);
    serializer.serialize_object(keys_);
    return serializer.get_serial();
  }
};