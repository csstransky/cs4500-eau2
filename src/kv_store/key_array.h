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

  KeyArray* clone() { return new KeyArray(*this); }

  /* Gets a Key at the given index */
  /* Throws an error if not found or out of range or no elements in array*/
  Key* get(size_t index) { return dynamic_cast<Key*>(ObjectArray::get(index)); }

  /* Adds an Key to the end of the Array, returns the new length */
  size_t push(Key* const to_add) { return ObjectArray::push(to_add); }

  /* Removes a Key at the given index, returns removed Key */
  /* Throws an error if not found or out of range or no elements in array*/
  Key* remove(size_t index) { return dynamic_cast<Key*>(ObjectArray::remove(index)); }

  /* Replaces a Key at the given index with the given Key, returns the replaced Key */
  /* Throws an error if not found or out of range or no elements in array*/
  Key* replace(size_t index, Key* const to_add) {
    return dynamic_cast<Key*>(ObjectArray::replace(index, to_add));
  }

  static KeyArray* deserialize(char* serial) {
    Deserializer deserializer(serial);
    return deserialize(deserializer);
  }

  static KeyArray* deserialize(Deserializer& deserializer) {
    Array* new_array = deserialize_new_array_(deserializer);
    KeyArray* new_key_array = static_cast<KeyArray*>(new_array);
    for (size_t ii = 0; ii < new_key_array->count_; ii++) {
      Key* new_object = Key::deserialize(deserializer);
      new_array->push(new_object);
      delete new_object;
    }
    return new_key_array;
  }
};