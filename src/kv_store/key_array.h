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

  KeyArray(Deserializer& deserializer) : ObjectArray(deserializer) {
    for (size_t ii = 0; ii < count_; ii++) elements_[ii].o = new Key(deserializer);
  }

  size_t push(Object* const to_add) { 
    assert(dynamic_cast<Key*>(to_add));
    return ObjectArray::push(to_add); 
  }

  KeyArray* clone() { return new KeyArray(*this); }
  Key* get(size_t index) { return static_cast<Key*>(ObjectArray::get(index)); }
  Key* remove(size_t index) { return static_cast<Key*>(ObjectArray::remove(index)); }
  Key* replace(size_t index, Key* const to_add) {return static_cast<Key*>(ObjectArray::replace(index, to_add)); }
};