//lang::CwC
#pragma once

#include "../helpers/array.h"
#include "key.h"

/**
 * An Array class to which Keys can be added to and removed from.
 * author: kaylindevchand & csstransky */
class KeyArray : public ObjectArray {
public:
  KeyArray() : KeyArray(1) { }
  KeyArray(const size_t size) : ObjectArray(size) { }
  KeyArray(KeyArray& arr) : ObjectArray(arr) { }
  KeyArray(char* serial) { 
    Deserializer deserializer(serial);
    deserialize_key_array_(deserializer);
  }
  KeyArray(Deserializer& deserializer) {
    deserialize_key_array_(deserializer);
  }

  void deserialize_key_array_(Deserializer& deserializer) {
    deserialize_basic_array_(deserializer);
    for (size_t ii = 0; ii < count_; ii++)
      elements_[ii].o = new Key(deserializer);
  }

  KeyArray* clone() { return new KeyArray(*this); }

  /* Gets a Key at the given index */
  /* Throws an error if not found or out of range or no elements in array*/
  Key* get(size_t index) { return static_cast<Key*>(ObjectArray::get(index)); }

  /* Adds an Key to the end of the Array, returns the new length */
  size_t push(Object* const to_add) { 
    assert(dynamic_cast<Key*>(to_add));
    return ObjectArray::push(to_add); 
  }

  /* Removes a Key at the given index, returns removed Key */
  /* Throws an error if not found or out of range or no elements in array*/
  Key* remove(size_t index) { return static_cast<Key*>(ObjectArray::remove(index)); }

  /* Replaces a Key at the given index with the given Key, returns the replaced Key */
  /* Throws an error if not found or out of range or no elements in array*/
  Key* replace(size_t index, Key* const to_add) {
    return static_cast<Key*>(ObjectArray::replace(index, to_add));
  }
};