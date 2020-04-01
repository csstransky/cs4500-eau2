//lang::CwC
#pragma once

#include "object.h"
#include "string.h"
#include <assert.h>
#include "payload.h"

// TODO: This has actually all been refactored all ready, and will be merged in with the next
// reduce_code branch
/**
 * An basic Array class that should be inherited, but not directly used.
 * Requested here: https://github.com/chasebish/cs4500_assignment1_part2/issues/2
 * author: chasebish */
class Array : public Object {
public:
  size_t size_;
  size_t count_;
  char type_;
  Payload* elements_;

  Array(char type, size_t size, size_t count) {
    assert(size > 0 && size >= count && (type == 'I' || type == 'B' || type == 'D' || type == 'O'));
    count_ = count;
    size_ = size;
    type_ = type;
    // TODO: Find a way to do this without using new
    elements_ = new Payload[size_];  
  }

  Array(char type, size_t size) : Array(type, size, 0) { }

  Array(char type) : Array(type, 1) { }

  Array(Array& arr) : Array(arr.type_, arr.size_, arr.count_) {
    for (size_t i = 0; i < count_; i++) {
      if (arr.type_ == 'O')
        elements_[i].o = arr.elements_[i].o ? arr.elements_[i].o->clone() : nullptr;
      else
        elements_[i] = arr.elements_[i];
    }
  }

  Array(char* serial) {
    Deserializer deserializer(serial);
    deserialize_basic_array_(deserializer);
  }

  Array(Deserializer& deserializer) {
    deserialize_basic_array_;
  }

  // IMPORTANT: ObjectArray subclasses will need to deserialize its own specific Objects to 
  // each of the indices in its elements_ (like StringArray)
  void deserialize_basic_array_(Deserializer& deserializer) {
    deserializer.deserialize_size_t(); // skip serial_size
    size_ = deserializer.deserialize_size_t();
    count_ = deserializer.deserialize_size_t();
    type_ = deserializer.deserialize_char();
    elements_ = new Payload[size_];
    for (size_t ii = 0; ii < count_ && type_ != 'O'; ii++) {
      switch(type_) {
        case 'I': elements_[ii].i = deserializer.deserialize_int(); break;
        case 'B': elements_[ii].b = deserializer.deserialize_bool(); break;
        case 'D': elements_[ii].d = deserializer.deserialize_double(); break;
      }
    }
  }

  ~Array() {
    if (type_ == 'O') {
      for (size_t ii = 0; ii < count_; ii++)
        delete elements_[ii].o;  
    }
    delete[] elements_;
  }

  bool equals(Object* const obj) {
    Array* arr = dynamic_cast<Array*>(obj); 
    if (!arr || type_ != arr->type_) return false;
    for (size_t i = 0; i < count_; i++) {
      switch(type_) {
        case 'I': if (elements_[i].i != arr->elements_[i].i) return false;
        case 'B': if (elements_[i].b != arr->elements_[i].b) return false;
        case 'D': if (elements_[i].d != arr->elements_[i].d) return false;
        case 'O': if (!elements_[i].o->equals(arr->elements_[i].o)) return false;
      }
    }
    return true;
  }

  Array* clone() { return new Array(*this); }

  size_t hash() { 
    size_t hash = 0;
    for (size_t i = 0; i < count_; i++) {
      switch(type_) {
        // We want to avoid 0 * 0, so we add 1 to both sides
        case 'I': hash += (elements_[i].i + 1) * (i + 1);
        case 'B': hash += (elements_[i].b + 1) * (i + 1);
        case 'D': hash += (elements_[i].d + 1) * (i + 1);
        case 'O': hash += (elements_[i].o->hash() + 1) * (i + 1);
      }
    }
    return hash;
  }

  void increase_array_() {
    size_ = size_ * 2;
    // TODO: Find a way to do this without new
    Payload* new_elements = new Payload[size_];
    for (size_t i = 0; i < count_; i++)
      new_elements[i] = elements_[i];
    delete[] elements_;
    elements_ = new_elements;
  }
  
  size_t length() { return count_;  }

  size_t push_payload(Payload to_add) {
    if (count_ + 1 > size_)
      increase_array_();
    elements_[count_] = to_add;
    return count_++;
  }

  Payload get_payload(size_t index) {
    assert(count_ > 0 && index < count_);
    return elements_[index];
  }

  size_t index_of_payload_(Payload payload) {
    for (size_t ii = 0; ii < count_; ii++) {
      switch(type_) {
        case 'I': if (elements_[ii].i == payload.i) return ii;
        case 'D': if (elements_[ii].d == payload.d) return ii;
        case 'B': if (elements_[ii].b == payload.b) return ii;
        case 'O': if (elements_[ii].o->equals(payload.o)) return ii;
      }
    }
    return -1;
  }

  Payload pop_payload_() {
    assert(count_ > 0);
    count_--;
    return elements_[count_];
  }

  Payload remove_payload_(size_t index) {
    assert(count_ > 0 && index < count_);
    Payload element = elements_[index];
    for (size_t i = index; i < count_ - 1; i++)
      elements_[i] = elements_[i + 1];
    count_--;
    return element;
  }

  void clear() {
    if (type_ == 'O') 
      for (size_t ii = 0; ii < count_; ii++) 
        delete elements_[ii].o;
    count_ = 0;
  }

  Payload replace_payload_(size_t index, Payload to_add) {
    assert(count_ > 0 && index < count_);
    Payload element = elements_[index];
    elements_[index] = to_add;
    return element;
  }

  size_t elements_serial_len_() {
    switch(type_) {
      case 'I': return count_ * sizeof(int);
      case 'D': return count_ * sizeof(double);
      case 'B': return count_ * sizeof(bool); 
      case 'O': {
        size_t elements_serial_length = 0;
        for (size_t ii = 0; ii < count_; ii++)
          elements_serial_length += elements_[ii].o->serial_len();
        return elements_serial_length;
      }
    }
  }

  size_t serial_len() {
    return sizeof(size_t) // serial_length
      + sizeof(size_t) // size_
      + sizeof(size_t) // count_
      + sizeof(char) // type_
      + elements_serial_len_();
  }

  char* serialize() {
    size_t serial_size = serial_len();
    Serializer serializer(serial_size);
    serializer.serialize_size_t(serializer.get_serial_size());
    serializer.serialize_size_t(size_);
    serializer.serialize_size_t(count_);
    serializer.serialize_char(type_);
    for (size_t ii = 0; ii < count_; ii++) {
      switch(type_) {
        case 'O': serializer.serialize_object(elements_[ii].o); break;
        case 'I': serializer.serialize_int(elements_[ii].i); break;
        case 'D': serializer.serialize_double(elements_[ii].d); break;
        case 'B': serializer.serialize_bool(elements_[ii].b); break;
      }
    }
    return serializer.get_serial();
  }
};

/**
 * An Array class to which bools can be added to and removed from.
 * author: chasebish */
class BoolArray : public Array {
public:
  BoolArray() : BoolArray(1) { }
  BoolArray(const size_t size) : Array('B', size) { }
  BoolArray(BoolArray& arr) : Array(arr) { }
  BoolArray(char* serial) : Array(serial) { }
  BoolArray(Deserializer& deserializer) : Array(deserializer) { }

  BoolArray* clone() { return new BoolArray(*this); }

  /** Adds a bool to the end of the Array, returns the new length */
  size_t push(bool to_add) { return push_payload(bool_to_payload(to_add)); }

  /** Gets a Boolean. Throws an error if not found or out of range or no elements in array */
  bool get(size_t index) { return get_payload(index).b; }

  /* Returns the index of the given bool, -1 if bool is not found */
  size_t index_of(bool to_find) { return index_of_payload_(bool_to_payload(to_find)); }

  /* Removes an bool at the given index, returns removed bool */
  /* Throws an error if not found or out of range or no elements in array*/
  bool remove(size_t index) { return remove_payload_(index).b; }

  /* Replaces an bool at the given index with the given bool, returns the replaced bool */
  /* Throws an error if not found or out of range or no elements in array*/
  bool replace(size_t index, bool to_add) {
    return replace_payload_(index, bool_to_payload(to_add)).b;
  }
};

/**
 * An Array class to which doubles can be added to and removed from.
 * author: chasebish */
class DoubleArray : public Array {
public:
  DoubleArray() : DoubleArray(1) { }
  DoubleArray(const size_t size) : Array('D', size) { }
  DoubleArray(DoubleArray& arr) : Array(arr) { }
  DoubleArray(char* serial) : Array(serial) { }
  DoubleArray(Deserializer& deserializer) : Array(deserializer) { }

  DoubleArray* clone() { return new DoubleArray(*this); }

  /** Adds a double to the end of the Array, returns the new length */
  size_t push(double to_add) { 
    return push_payload(double_to_payload(to_add)); }

  /** Gets a double. Throws an error if not found or out of range  or no elements in array*/
  double get(size_t index) { return get_payload(index).d; }

  /* Returns the index of the given double, -1 if double is not found */
  size_t index_of(double to_find) { return index_of_payload_(double_to_payload(to_find)); }

  /* Removes a double at the given index, returns removed double */
  /* Throws an error if not found or out of range or no elements in array*/  
  double remove(size_t index) { return remove_payload_(index).d; }

  /* Replaces a double at the given index with the given double, returns the replaced double */
  /* Throws an error if not found or out of range or no elements in array*/
  double replace(size_t index, double to_add) {
    return replace_payload_(index, double_to_payload(to_add)).d;
  }
};

/**
 * An Array class to which ints can be added to and removed from.
 * author: chasebish */
class IntArray : public Array {
public:
  IntArray() : IntArray(1) { }
  IntArray(const size_t size) : Array('I', size) { }
  IntArray(IntArray& arr) : Array(arr) { }
  IntArray(char* serial) : Array(serial) { }
  IntArray(Deserializer& deserializer) : Array(deserializer) { }

  IntArray* clone() { return new IntArray(*this); }

  /** Adds an int to the end of the Array, returns the new length */
  size_t push(int to_add) { return push_payload(int_to_payload(to_add)); }

  /** Gets an int. Throws an error if not found or out of range or no elements in array */
  int get(size_t index) { return get_payload(index).i; }

  /* Returns the index of the given int, -1 if int is not found */
  size_t index_of(int to_find) { return index_of_payload_(int_to_payload(to_find)); }

  /* Removes an int at the given index, returns removed int */
  /* Throws an error if not found or out of range or no elements in array*/
  int remove(size_t index) { return remove_payload_(index).i; }

  /* Replaces an int at the given index with the given int, returns the replaced int */
  /* Throws an error if not found or out of range or no elements in array*/
  int replace(size_t index, int to_add) {
    return replace_payload_(index, int_to_payload(to_add)).i;
  }
};

/**
 * An Array class to which Objects can be added to and removed from.
 * author: chasebish & csstransky */
class ObjectArray : public Array {
public:
  ObjectArray() : ObjectArray(1) { }
  ObjectArray(size_t size) : Array('O', size) { }
  ObjectArray(ObjectArray& arr) : Array(arr) { }

  ObjectArray* clone() { return new ObjectArray(*this); }

 /** Adds a Object to the end of the Array, returns the new length */
  virtual size_t push(Object* const to_add) { 
    Object* object_clone = to_add ? to_add->clone() : nullptr;
    return push_payload(object_to_payload(object_clone)); 
  }

  /** Gets an Object. Throws an error if not found or out of range or no elements in array */
  Object* get(size_t index) { return get_payload(index).o; }

  /* Returns the index of the given Object, -1 if Object is not found */
  size_t index_of(Object* to_find) { return index_of_payload_(object_to_payload(to_find)); }

  /* Removes a Object at the given index, returns removed Object */
  /* Throws an error if not found or out of range or no elements in array*/
  Object* remove(size_t index) { return remove_payload_(index).o; }

  /* Replaces a Object at the given index with the given Object, returns the replaced Object */
  /* Throws an error if not found or out of range or no elements in array*/
  Object* replace(size_t index, Object* to_add) {
    Object* object_clone = to_add ? to_add->clone() : nullptr;
    return replace_payload_(index, object_to_payload(object_clone)).o;
  }
};

/**
 * An Array class to which Strings can be added to and removed from.
 * author: chasebish */
class StringArray : public ObjectArray {
public:
  StringArray() : StringArray(1) {  }
  StringArray(size_t size) : ObjectArray(size) { }
  StringArray(StringArray& arr) : ObjectArray(arr) { }
  StringArray(char* serial) { 
    Deserializer deserializer(serial);
    deserialize_string_array_(deserializer);
  }
  StringArray(Deserializer& deserializer) {
    deserialize_string_array_(deserializer);
  }

  void deserialize_string_array_(Deserializer& deserializer) {
    deserialize_basic_array_(deserializer);
    for (size_t ii = 0; ii < count_; ii++)
      elements_[ii].o = new String(deserializer);
  }

  StringArray* clone() { return new StringArray(*this); }

  /* Adds an String to the end of the Array, returns the new length */
  size_t push(Object* const to_add) { 
    assert(dynamic_cast<String*>(to_add));
    return ObjectArray::push(to_add); 
  }

  /* Gets String. Throws an error if not found or out of range or no elements in array*/
  String* get(size_t index) { return static_cast<String*>(ObjectArray::get(index)); }

  /* Removes a String at the given index, returns removed String */
  /* Throws an error if not found or out of range or no elements in array*/
  String* remove(size_t index) { return static_cast<String*>(ObjectArray::remove(index)); }

  /* Replaces a String at the given index with the given String, returns the replaced String */
  /* Throws an error if not found or out of range or no elements in array*/
  String* replace(size_t index, String* const to_add) {
    return static_cast<String*>(ObjectArray::replace(index, to_add));
  }
};