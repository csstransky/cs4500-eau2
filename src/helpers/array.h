// Made by Kaylin Devchand and Cristian Stransky
#pragma once

#include "object.h"
#include "string.h"
#include <assert.h>
#include "payload.h"

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

  Array(Deserializer& deserializer) {
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
        case 'I': if (elements_[i].i != arr->elements_[i].i) return false; break;
        case 'B': if (elements_[i].b != arr->elements_[i].b) return false; break;
        case 'D': if (elements_[i].d != arr->elements_[i].d) return false; break;
        case 'O': if (!elements_[i].o->equals(arr->elements_[i].o)) return false; break;
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

  /** returns the new size **/
  size_t push(Payload to_add) {
    if (count_ + 1 > size_)
      increase_array_();
    elements_[count_] = to_add;
    return count_++;
  }

  Payload get(size_t index) {
    assert(count_ > 0 && index < count_);
    return elements_[index];
  }

  /** returns -1 if not found **/
  size_t index_of(Payload payload) {
    for (size_t ii = 0; ii < count_; ii++) {
      switch(type_) {
        case 'I': if (elements_[ii].i == payload.i) return ii; break;
        case 'D': if (elements_[ii].d == payload.d) return ii; break;
        case 'B': if (elements_[ii].b == payload.b) return ii; break;
        case 'O': if (elements_[ii].o->equals(payload.o)) return ii; break;
      }
    }
    return -1;
  }

  Payload remove(size_t index) {
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

  Payload replace(size_t index, Payload to_add) {
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

class BoolArray : public Array {
public:
  BoolArray() : BoolArray(1) { }
  BoolArray(const size_t size) : Array('B', size) { }
  BoolArray(BoolArray& arr) : Array(arr) { }
  BoolArray(Deserializer& deserializer) : Array(deserializer) { }
  BoolArray* clone() { return new BoolArray(*this); }
  size_t push(bool to_add) { return Array::push(bool_to_payload(to_add)); }
  bool get(size_t index) { return Array::get(index).b; }
  size_t index_of(bool to_find) { return Array::index_of(bool_to_payload(to_find)); }
  bool remove(size_t index) { return Array::remove(index).b; }
  bool replace(size_t index, bool to_add) { return Array::replace(index, bool_to_payload(to_add)).b; }
};

class DoubleArray : public Array {
public:
  DoubleArray() : DoubleArray(1) { }
  DoubleArray(const size_t size) : Array('D', size) { }
  DoubleArray(DoubleArray& arr) : Array(arr) { }
  DoubleArray(Deserializer& deserializer) : Array(deserializer) { }
  DoubleArray* clone() { return new DoubleArray(*this); }
  size_t push(double to_add) { return Array::push(double_to_payload(to_add)); }
  double get(size_t index) { return Array::get(index).d; }
  size_t index_of(double to_find) { return Array::index_of(double_to_payload(to_find)); }
  double remove(size_t index) { return Array::remove(index).d; }
  double replace(size_t index, double to_add) { return Array::replace(index, double_to_payload(to_add)).d;
  }
};

class IntArray : public Array {
public:
  IntArray() : IntArray(1) { }
  IntArray(const size_t size) : Array('I', size) { }
  IntArray(IntArray& arr) : Array(arr) { }
  IntArray(Deserializer& deserializer) : Array(deserializer) { }
  IntArray* clone() { return new IntArray(*this); }
  size_t push(int to_add) { return Array::push(int_to_payload(to_add)); }
  int get(size_t index) { return Array::get(index).i; }
  size_t index_of(int to_find) { return Array::index_of(int_to_payload(to_find)); }
  int remove(size_t index) { return Array::remove(index).i; }
  int replace(size_t index, int to_add) { return Array::replace(index, int_to_payload(to_add)).i;
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
  ObjectArray(Deserializer deserializer) : Array(deserializer) { }
  ObjectArray* clone() { return new ObjectArray(*this); }
  virtual size_t push(Object* const to_add) { return Array::push(object_to_payload(to_add ? to_add->clone() : nullptr)); }
  Object* get(size_t index) { return Array::get(index).o; }
  size_t index_of(Object* to_find) { return Array::index_of(object_to_payload(to_find)); }
  Object* remove(size_t index) { return Array::remove(index).o; }
  Object* replace(size_t index, Object* to_add) {
    return Array::replace(index, object_to_payload(to_add ? to_add->clone() : nullptr)).o;
  }
};

class StringArray : public ObjectArray {
public:
  StringArray() : StringArray(1) {  }
  StringArray(size_t size) : ObjectArray(size) { }
  StringArray(StringArray& arr) : ObjectArray(arr) { }

  StringArray(Deserializer& deserializer) : ObjectArray(deserializer) {
    for (size_t ii = 0; ii < count_; ii++) elements_[ii].o = new String(deserializer);
  }

  StringArray* clone() { return new StringArray(*this); }

  size_t push(Object* const to_add) { 
    assert(dynamic_cast<String*>(to_add));
    return ObjectArray::push(to_add); 
  }

  String* get(size_t index) { return static_cast<String*>(ObjectArray::get(index)); }

  String* remove(size_t index) { return static_cast<String*>(ObjectArray::remove(index)); }

  String* replace(size_t index, String* const to_add) {
    return static_cast<String*>(ObjectArray::replace(index, to_add));
  }
};