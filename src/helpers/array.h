//lang::CwC
#pragma once

#include "object.h"
#include "string.h"
#include <assert.h>

/**
 * An basic Array class that should be inherited, but not directly used.
 * Requested here: https://github.com/chasebish/cs4500_assignment1_part2/issues/2
 * author: chasebish */
class Array : public Object {
public:
  size_t size_;
  size_t count_;
  /** CONSTRUCTORS & DESTRUCTORS **/

  /* Creates a default Array */
  Array() : Array(1) { }

  /* Creates an Array of desired size */
  Array(size_t size) {
    assert(size > 0);
    count_ = 0;
    size_ = size;
  }

  /* Clears Array from memory */
  virtual ~Array() {

  }

  /** INHERITED METHODS **/

  /* Inherited from Object, checks equality between an Array and an Object */
  bool equals(Object* const obj) {
    return false;
  }

  /* Inherited from Object, generates a hash for an Array */
  size_t hash() {
    return 0;
  }

  /** ARRAY METHODS **/
  
  /* Removes all elements from the Array */
  virtual void clear() {
    count_ = 0;
  }

  /* Returns the current length of the contents in an Array */
  size_t length() {
    return count_;
  }

  // NOTE: Only the base serial size, NOT including the elements of the array
  size_t base_array_serial_size_() {
    return sizeof(size_t) // serial_length
      + sizeof(size_t) // size_
      + sizeof(size_t); // count_
  }

  void serialize_array_(Serializer& serializer) {
    serializer.serialize_size_t(serializer.get_serial_size());
    serializer.serialize_size_t(size_);
    serializer.serialize_size_t(count_);
  }
};

/**
 * An Array class to which bools can be added to and removed from.
 * author: chasebish */
class BoolArray : public Array {
public:
  bool* elements_;

  /* Doubles the capacity of the array */
  void increase_array_() {
    bool* s = new bool[size_ * 2];
    for (size_t i = 0; i < count_; i++) {
        s[i] = elements_[i];
    }
        
    delete[] elements_;
    elements_ = s;
    size_ = size_ * 2;
  }
  /** CONSTRUCTORS & DESTRUCTORS **/

  /* Creates a default Array */
  BoolArray() : BoolArray(1) { }

  /* Creates an Array of desired size */
  BoolArray(const size_t size) : Array(size) {
    elements_ = new bool[size];
  }

  /* Copies the contents of an already existing Array */
  BoolArray(BoolArray* const arr) : BoolArray(arr->size_) {
    for (size_t i = 0; i < arr->length(); i++) {
      elements_[i] = arr->get(i);
    }
    count_ = arr->length();
  }

  /* Clears Array from memory */
  ~BoolArray() {
    delete[] elements_;
  }

  /** INHERITED METHODS **/

  /* Inherited from Object, checks equality between an Array and an Object */
  bool equals(Object* const obj) {
    if (obj == nullptr) {
      return false;
    }

    BoolArray* arr = dynamic_cast<BoolArray*>(obj); 

    if (arr == nullptr) {
      return false;
    }

    for (size_t i = 0; i < count_; i++) {
      if (elements_[i] != arr->get(i)) {
        return false;
      }
    }

    return true;
  }

  BoolArray* clone() {
    return new BoolArray(this);
  }

  /* Inherited from Object, generates a hash for an Array */
  size_t hash() {
    size_t hash = 0; //= reinterpret_cast<size_t>(this);
    
    for (size_t i = 0; i < count_; i++) {
      hash += elements_[i] + i;
    }

    return hash;
  }

  /** ARRAY METHODS **/

  /* Adds an Array to existing contents */
  void concat(BoolArray* const arr) {
    while (count_ + arr->length() > size_) {
      increase_array_();
    }
    
    for (size_t i = 0; i < arr->length(); i++) {
      elements_[count_ + i] = arr->get(i);
    }

    count_ = count_ + arr->length();
  }

  /**
   * Gets an bool at the given index
   * Throws an error if not found or out of range or no elements in array */
  bool get(size_t index) {
    assert(count_ > 0 && index < count_);

    return elements_[index];
  }

  /* Returns the index of the given bool, -1 if bool is not found */
  size_t index_of(bool to_find) {
    for (size_t i = 0; i < count_; i++) {
      if (elements_[i] == to_find) {
        return i;
      }
    }

    return -1;    
  }

  /* Removes the last bool of the Array, returns the removed bool */
  /* Throws an error if not found or out of range or no elements in array*/
  bool pop() {
    assert(count_ > 0);

    bool e = elements_[count_ - 1];
    count_--;
    return e;
  }

  /* Adds an bool to the end of the Array, returns the new length */
  size_t push(bool to_add) {
    if (count_ + 1 > size_) {
      increase_array_();
    }

    elements_[count_] = to_add;
    return count_++;
  }

  /* Removes an bool at the given index, returns removed bool */
  /* Throws an error if not found or out of range or no elements in array*/
  bool remove(size_t index) {
    assert(count_ > 0 && index < count_);

    bool e = elements_[index];
    for (size_t i = index; i < count_ - 1; i++) {
      elements_[i] = elements_[i + 1];
    }
    count_--;
    return e;
  }

  /* Replaces an bool at the given index with the given bool, returns the replaced bool */
  /* Throws an error if not found or out of range or no elements in array*/
  bool replace(size_t index, bool to_add) {
    assert(count_ > 0 && index < count_);

    bool e = elements_[index];
    elements_[index] = to_add;

    return e;
  }

  size_t serial_len() {
    // Includes the beginning serial size, size of array, count of array, then elements
    return base_array_serial_size_() + count_ * sizeof(bool);
  }

  char* serialize() {
    size_t serial_size = serial_len();
    Serializer serializer(serial_size);
    serialize_array_(serializer);
    for (size_t ii = 0; ii < count_; ii++){
        serializer.serialize_bool(elements_[ii]);
    }
    return serializer.get_serial();
  }

  static BoolArray* deserialize(char* serial) {
      Deserializer deserializer(serial);
      return deserialize(deserializer);
  }

  static BoolArray* deserialize(Deserializer& deserializer) {
      // Don't need serial size, so we skip it
      deserializer.deserialize_size_t();
      size_t size = deserializer.deserialize_size_t();
      size_t count = deserializer.deserialize_size_t();
      BoolArray* new_array = new BoolArray(size);
      for (size_t ii = 0; ii < count; ii++) {
        new_array->push(deserializer.deserialize_bool());
      }
      return new_array;
  }
};

/**
 * An Array class to which floats can be added to and removed from.
 * author: chasebish */
class FloatArray : public Array {
public:
  float* elements_;
  
  /* Doubles the capacity of the array */
  void increase_array_() {
    float* s = new float[size_ * 2];
    for (size_t i = 0; i < count_; i++) {
        s[i] = elements_[i];
    }
        
    delete[] elements_;
    elements_ = s;
    size_ = size_ * 2;
  }

  /** CONSTRUCTORS & DESTRUCTORS **/

  /* Creates a default Array */
  FloatArray() : FloatArray(1) { }

  /* Creates an Array of desired size */
  FloatArray(const size_t size) : Array(size) {
    elements_ = new float[size];
  }

  /* Copies the contents of an already existing Array */
  FloatArray(FloatArray* const arr) : FloatArray(arr->size_) {
    for (size_t i = 0; i < arr->length(); i++) {
      elements_[i] = arr->get(i);
    }
    count_ = arr->length();
  }

  /* Clears Array from memory */
  ~FloatArray() {
    delete[] elements_;
  }

  /** INHERITED METHODS **/

  /* Inherited from Object, checks equality between an Array and an Object */
  bool equals(Object* const obj) {
    if (obj == nullptr) {
      return false;
    }

    FloatArray* arr = dynamic_cast<FloatArray*>(obj); 

    if (arr == nullptr) {
      return false;
    }

    for (size_t i = 0; i < count_; i++) {
      if (elements_[i] != arr->get(i)) {
        return false;
      }
    }

    return true;
  }

  FloatArray* clone() {
    return new FloatArray(this);
  }

  /* Inherited from Object, generates a hash for an Array */
  size_t hash() {
    size_t hash = 0; //= reinterpret_cast<size_t>(this);
    
    for (size_t i = 0; i < count_; i++) {
      hash += elements_[i] + i;
    }

    return hash;
  }

  /** ARRAY METHODS **/

    /* Adds an Array to existing contents */
  void concat(FloatArray* const arr) {
    while (count_ + arr->length() > size_) {
      increase_array_();
    }
    
    for (size_t i = 0; i < arr->length(); i++) {
      elements_[count_ + i] = arr->get(i);
    }

    count_ = count_ + arr->length();
  }

  /**
   * Gets an bool at the given index
   * Throws an error if not found or out of range  or no elements in array*/
  float get(size_t index) {
    assert(count_ > 0 && index < count_);

    return elements_[index];
  }

  /* Returns the index of the given float, -1 if float is not found */
  size_t index_of(float to_find) {
    for (size_t i = 0; i < count_; i++) {
      if (elements_[i] == to_find) {
        return i;
      }
    }

    return -1;    
  }

  /* Removes the last float of the Array, returns the removed float */
  /* Throws an error if not found or out of range or no elements in array*/
  float pop() {
    assert(count_ > 0);
    float e = elements_[count_ - 1];
    count_--;
    return e;
  }

  /* Adds an float to the end of the Array, returns the new length */
  size_t push(float to_add) {
    if (count_ + 1 > size_) {
      increase_array_();
    }

    elements_[count_] = to_add;
    return count_++;
  }

  /* Removes an float at the given index, returns removed float */
  /* Throws an error if not found or out of range or no elements in array*/  
  float remove(size_t index) {
    assert(count_ > 0 && index < count_);

    float e = elements_[index];
    for (size_t i = index; i < count_ - 1; i++) {
      elements_[i] = elements_[i + 1];
    }
    count_--;
    return e;
  }

  /* Replaces a float at the given index with the given float, returns the replaced float */
  /* Throws an error if not found or out of range or no elements in array*/
  float replace(size_t index, float to_add) {
    assert(count_ > 0 && index < count_);

    float e = elements_[index];
    elements_[index] = to_add;

    return e;
  }

  size_t serial_len() {
    // Includes the beginning serial size, size of array, count of array, then elements
    return base_array_serial_size_() + count_ * sizeof(float);
  }

  char* serialize() {
    size_t serial_size = serial_len();
    Serializer serializer(serial_size);
    serialize_array_(serializer);
    for (size_t ii = 0; ii < count_; ii++){
        serializer.serialize_float(elements_[ii]);
    }
    return serializer.get_serial();
  }

  static FloatArray* deserialize(char* serial) {
      Deserializer deserializer(serial);
      return deserialize(deserializer);
  }

  static FloatArray* deserialize(Deserializer& deserializer) {
      // Don't need serial size, so we skip it
      deserializer.deserialize_size_t();
      size_t size = deserializer.deserialize_size_t();
      size_t count = deserializer.deserialize_size_t();
      FloatArray* new_array = new FloatArray(size);
      for (size_t ii = 0; ii < count; ii++) {
        new_array->push(deserializer.deserialize_float());
      }
      return new_array;
  }
};

/**
 * An Array class to which ints can be added to and removed from.
 * author: chasebish */
class IntArray : public Array {
public:
  int* elements_;
  
  /* Doubles the capacity of the array */
  void increase_array_() {
    int* s = new int[size_ * 2];
    for (size_t i = 0; i < count_; i++) {
        s[i] = elements_[i];
    }
        
    delete[] elements_;
    elements_ = s;
    size_ = size_ * 2;
  }

  /** CONSTRUCTORS & DESTRUCTORS **/

  /* Creates a default Array */
  IntArray() : IntArray(1) { }

  /* Creates an Array of desired size */
  IntArray(const size_t size) : Array(size) {
    elements_ = new int[size];
  }

  /* Copies the contents of an already existing Array */
  IntArray(IntArray* arr) : IntArray(arr->size_) {
    for (size_t i = 0; i < arr->length(); i++) {
      elements_[i] = arr->get(i);
    }
    count_ = arr->length();
  }

  /* Clears Array from memory */
  ~IntArray() {
    delete[] elements_;
  }

  /** INHERITED METHODS **/

  /* Inherited from Object, checks equality between an Array and an Object */
  bool equals(Object* const obj) {
    if (obj == nullptr) {
      return false;
    }

    IntArray* arr = dynamic_cast<IntArray*>(obj); 

    if (arr == nullptr) {
      return false;
    }

    for (size_t i = 0; i < count_; i++) {
      if (elements_[i] != arr->get(i)) {
        return false;
      }
    }

    return true;
  }

  IntArray* clone() {
    return new IntArray(this);
  }

  /* Inherited from Object, generates a hash for an Array */
  size_t hash() {
    size_t hash = 0; //= reinterpret_cast<size_t>(this);
    
    for (size_t i = 0; i < count_; i++) {
      hash += elements_[i] + i;
    }

    return hash;
  }

  /** ARRAY METHODS **/

    /* Adds an Array to existing contents */
  void concat(IntArray* arr) {
    while (count_ + arr->length() > size_) {
      increase_array_();
    }
    
    for (size_t i = 0; i < arr->length(); i++) {
      elements_[count_ + i] = arr->get(i);
    }

    count_ = count_ + arr->length();
  }

  /**
   * Gets an bool at the given index
   * Throws an error if not found or out of range or no elements in array */
  int get(size_t index) {
    assert(count_ > 0 && index < count_);

    return elements_[index];
  }

  /* Returns the index of the given int, -1 if int is not found */
  size_t index_of(int to_find) {
    for (size_t i = 0; i < count_; i++) {
      if (elements_[i] == to_find) {
        return i;
      }
    }

    return -1;    
  }

  /* Removes the last int of the Array, returns the removed int */
  /* Throws an error if not found or out of range or no elements in array*/
  int pop() {
    assert(count_ > 0);
    size_t e = elements_[count_ - 1];
    count_--;
    return e;
  }

  /* Adds an int to the end of the Array, returns the new length */
  size_t push(int to_add) {
    if (count_ + 1 > size_) {
      increase_array_();
    }

    elements_[count_] = to_add;
    return count_++;
  }

  /* Removes an int at the given index, returns removed int */
  /* Throws an error if not found or out of range or no elements in array*/
  int remove(size_t index) {
    assert(count_ > 0 && index < count_);

    int e = elements_[index];
    for (size_t i = index; i < count_ - 1; i++) {
      elements_[i] = elements_[i + 1];
    }
    count_--;
    return e;
  }

  /* Replaces an int at the given index with the given int, returns the replaced int */
  /* Throws an error if not found or out of range or no elements in array*/
  int replace(size_t index, int to_add) {
    assert(count_ > 0 && index < count_);

    int e = elements_[index];
    elements_[index] = to_add;

    return e;
  }

  size_t serial_len() {
    // Includes the beginning serial size, size of array, count of array, then elements
    return base_array_serial_size_() + count_ * sizeof(int);
  }

  char* serialize() {
    size_t serial_size = serial_len();
    Serializer serializer(serial_size);
    serialize_array_(serializer);
    for (size_t ii = 0; ii < count_; ii++){
        serializer.serialize_int(elements_[ii]);
    }
    return serializer.get_serial();
  }

  static IntArray* deserialize(char* serial) {
      Deserializer deserializer(serial);
      return deserialize(deserializer);
  }

  static IntArray* deserialize(Deserializer& deserializer) {
      // Don't need serial size, so we skip it
      deserializer.deserialize_size_t();
      size_t size = deserializer.deserialize_size_t();
      size_t count = deserializer.deserialize_size_t();
      IntArray* new_array = new IntArray(size);
      for (size_t ii = 0; ii < count; ii++) {
        new_array->push(deserializer.deserialize_int());
      }
      return new_array;
  }
};

/**
 * An Array class to which Objects can be added to and removed from.
 * author: chasebish & csstransky */
class ObjectArray : public Array {
public:
  Object** elements_;

  /* Doubles the capacity of the array */
  void increase_array_() {
    Object** s = new Object*[size_ * 2];
    for (size_t i = 0; i < count_; i++) {
        s[i] = elements_[i];
    }
        
    delete[] elements_;
    elements_ = s;
    size_ = size_ * 2;
  }
  /** CONSTRUCTORS & DESTRUCTORS **/

  /* Creates a default Array */
  ObjectArray() : ObjectArray(1) { }

  /* Creates an Array of desired size */
  ObjectArray(size_t size) : Array(size) {
    elements_ = new Object*[size];
  }

  /* Copies the contents of an already existing Array */
  ObjectArray(ObjectArray* const arr) : ObjectArray(arr->size_) {
    for (size_t i = 0; i < arr->length(); i++) {
      elements_[i] = arr->get(i)->clone();
    }
    count_ = arr->length();
  }

  /* Clears Array from memory */
  ~ObjectArray() {
    for (size_t ii = 0; ii < count_; ii++) {
      delete elements_[ii];
    }
    delete[] elements_;
  }

  void clear() {
    for (size_t ii = 0; ii < count_; ii++) {
      delete elements_[ii];
    }
    count_ = 0;
  }

  /** INHERITED METHODS **/

  /* Inherited from Object, checks equality between an Array and an Object */
  bool equals(Object* const obj) {
    if (obj == nullptr) {
      return false;
    }

    ObjectArray* arr = dynamic_cast<ObjectArray*>(obj); 

    if (arr == nullptr) {
      return false;
    }

    for (size_t i = 0; i < count_; i++) {
      if (!elements_[i]->equals(arr->get(i))) {
        return false;
      }
    }

    return true;
  }

  ObjectArray* clone() {
    return new ObjectArray(this);
  }

  /* Inherited from Object, generates a hash for an Array */
  size_t hash() {
    size_t hash = 0; //= reinterpret_cast<size_t>(this);
    
    for (size_t i = 0; i < count_; i++) {
      hash += elements_[i]->hash() + i;
    }

    return hash;
  }

  /** ARRAY METHODS **/

  /* Adds an ObjectArray to existing contents */
  virtual void concat(ObjectArray* const arr) {
    while (count_ + arr->length() > size_) {
      increase_array_();
    }
    
    for (size_t i = 0; i < arr->length(); i++) {
      elements_[count_ + i] = arr->get(i)->clone();
    }

    count_ = count_ + arr->length();
  }

  /* Returns the index of the given Object, -1 if Object is not found */
  size_t index_of(Object* const to_find) {
    for (size_t i = 0; i < count_; i++) {
      if (elements_[i] == to_find) {
        return i;
      }
    }

    return -1;   
  }

  /* Removes the last Object of the Array, returns the removed Object */
  /* Throws an error if not found or out of range or no elements in array*/
  virtual Object* pop() {
    assert(count_ > 0);
    Object* e = elements_[count_ - 1];
    count_--;
    return e;
  }

  /**
   * Gets an Object at the given index
   * Throws an error if not found or out of range or no elements in array */
  virtual Object* get(size_t index) {
    assert(count_ > 0 && index < count_);

    return elements_[index];
  }

  /* Adds a Object to the end of the Array, returns the new length */
  virtual size_t push(Object* const to_add) {
    if (count_ + 1 > size_) {
      increase_array_();
    }

    elements_[count_] = to_add ? to_add->clone() : nullptr;
    return count_++;
  }

  /* Removes a Object at the given index, returns removed Object */
  /* Throws an error if not found or out of range or no elements in array*/
  virtual Object* remove(size_t index) {
    assert(count_ > 0 && index < count_);

    Object* e = elements_[index];
    for (size_t i = index; i < count_ - 1; i++) {
      elements_[i] = elements_[i + 1];
    }
    count_--;
    return e;
  }

  /* Replaces a Object at the given index with the given Object, returns the replaced Object */
  /* Throws an error if not found or out of range or no elements in array*/
  virtual Object* replace(size_t index, Object* const to_add) {
    assert(count_ > 0 && index < count_);

    Object* e = elements_[index];
    elements_[index] = to_add ? to_add->clone() : nullptr;

    return e;
  }

  size_t serial_len() {
    size_t elements_serial_length = 0;
    for (size_t ii = 0; ii < count_; ii++) {
      elements_serial_length += elements_[ii]->serial_len();
    }

    // Includes the beginning serial size, size of array, count of array, then elements
    return base_array_serial_size_() + elements_serial_length;
  }

  char* serialize() {
    size_t serial_size = serial_len();
    Serializer serializer(serial_size);
    serialize_array_(serializer);
    for (size_t ii = 0; ii < count_; ii++){
        serializer.serialize_object(elements_[ii]);
    }
    return serializer.get_serial();
  }

  static ObjectArray* deserialize(char* serial) {
      Deserializer deserializer(serial);
      return deserialize(deserializer);
  }

  // NOTE: Doesn't really work 
  static ObjectArray* deserialize(Deserializer& deserializer) {
    // Because of the static nature of the deserialize, we can't just find out what the object is
    // by simply looking at the serial itself. 

    // Example:
        // ObjectArray* new_array = new ObjectArray(size);
        // for (size_t ii = 0; ii < count; ii++) {
        //   // This will NOT work because it will NOT deserialize dynamically
        //   Object* new_object = Object::deserialize(deserializer);
        //   new_array->push(new_object);
        //   delete new_object;
        // }

    // It's perhaps possible, but is currently out of the scope of this project, so we will ignore 
    // the deserializing of a pure ObjectArray for now. Deserializing a StringArray is our objective
    assert(0);
  }
};

/**
 * An Array class to which Strings can be added to and removed from.
 * author: chasebish */
class StringArray : public ObjectArray {
public:
  /** CONSTRUCTORS & DESTRUCTORS **/

  /* Creates a default Array */
  StringArray() : StringArray(1) {
    
  }

  /* Creates an Array of desired size */
  StringArray(const size_t size) : ObjectArray(size) {
    
  }

  /* Copies the contents of an already existing Array */
  StringArray(StringArray* arr) : ObjectArray(arr) {
    
  }

  /* Clears Array from memory */
  ~StringArray() {

  }

  /** ARRAY METHODS **/

  StringArray* clone() {
    return new StringArray(this);
  }

  /* Adds an StringArray to existing contents */
  void concat(StringArray* const arr) {
    ObjectArray::concat(arr);
  }

  /* Gets a String at the given index */
  /* Throws an error if not found or out of range or no elements in array*/
  String* get(size_t index) {
    return dynamic_cast<String*>(ObjectArray::get(index));
  }

  /* Removes the last String of the Array, returns the removed String */
  /* Throws an error if not found or out of range or no elements in array*/
  String* pop() {
    return dynamic_cast<String*>(ObjectArray::pop());
  }

  /* Adds an String to the end of the Array, returns the new length */
  size_t push(String* const to_add) {
    return ObjectArray::push(to_add);
  }

  /* Removes a String at the given index, returns removed String */
  /* Throws an error if not found or out of range or no elements in array*/
  String* remove(size_t index) {
    return dynamic_cast<String*>(ObjectArray::remove(index));
  }

  /* Replaces a String at the given index with the given String, returns the replaced String */
  /* Throws an error if not found or out of range or no elements in array*/
  String* replace(size_t index, String* const to_add) {
    return dynamic_cast<String*>(ObjectArray::replace(index, to_add));
  }

  static StringArray* deserialize(char* serial) {
      Deserializer deserializer(serial);
      return deserialize(deserializer);
  }

  static StringArray* deserialize(Deserializer& deserializer) {
      // Don't need serial size, so we skip it
      deserializer.deserialize_size_t();
      size_t size = deserializer.deserialize_size_t();
      size_t count = deserializer.deserialize_size_t();
      StringArray* new_array = new StringArray(size);
      for (size_t ii = 0; ii < count; ii++) {
        String* new_object = String::deserialize(deserializer);
        new_array->push(new_object);
        delete new_object;
      }
      return new_array;
  }
};