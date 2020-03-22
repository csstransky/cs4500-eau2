//lang::CwC
#pragma once

#include "../helpers/array.h"
#include "key.h"

/**
 * An Array class to which Keys can be added to and removed from.
 * author: kaylindevchand & csstransky */
class KeyArray : public ObjectArray {
public:
  /** CONSTRUCTORS & DESTRUCTORS **/

  /* Creates a default Array */
  KeyArray() : KeyArray(1) {
      
  }

  /* Creates an Array of desired size */
  KeyArray(const size_t size) : ObjectArray(size) {
    
  }

  /* Copies the contents of an already existing Array */
  KeyArray(KeyArray* arr) : ObjectArray(arr) {
    
  }

  /* Clears Array from memory */
  ~KeyArray() {

  }

  /** ARRAY METHODS **/

  KeyArray* clone() {
    return new KeyArray(this);
  }

  /* Adds an KeyArray to existing contents */
  void concat(KeyArray* const arr) {
    ObjectArray::concat(arr);
  }

  /* Gets a Key at the given index */
  /* Throws an error if not found or out of range or no elements in array*/
  Key* get(size_t index) {
    return dynamic_cast<Key*>(ObjectArray::get(index));
  }

  /* Removes the last Key of the Array, returns the removed Key */
  /* Throws an error if not found or out of range or no elements in array*/
  Key* pop() {
    return dynamic_cast<Key*>(ObjectArray::pop());
  }

  /* Adds an Key to the end of the Array, returns the new length */
  size_t push(Key* const to_add) {
    return ObjectArray::push(to_add);
  }

  /* Removes a Key at the given index, returns removed Key */
  /* Throws an error if not found or out of range or no elements in array*/
  Key* remove(size_t index) {
    return dynamic_cast<Key*>(ObjectArray::remove(index));
  }

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
      // Don't need serial size, so we skip it
      deserializer.deserialize_size_t();
      size_t size = deserializer.deserialize_size_t();
      size_t count = deserializer.deserialize_size_t();
      KeyArray* new_array = new KeyArray(size);
      for (size_t ii = 0; ii < count; ii++) {
        Key* new_object = Key::deserialize(deserializer);
        new_array->push(new_object);
        delete new_object;
      }
      return new_array;
  }
};