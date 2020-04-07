#pragma once

#include "../helpers/array.h"
#include "column.h"

/**
 * An Array class to which Columns can be added to and removed from.
 * author: kaylindevchand & csstransky */
class ColumnArray : public ObjectArray {
public:
  ColumnArray() : ColumnArray(1) { }
  ColumnArray(const size_t size) : ObjectArray(size) { }
  ColumnArray(ColumnArray& arr) : ObjectArray(arr) { }

  ColumnArray(Deserializer& deserializer, KV_Store* kv_store) : ObjectArray(deserializer) {
    for (size_t ii = 0; ii < count_; ii++) elements_[ii].o = new Column(deserializer, kv_store);
  }

  size_t push(Object* const to_add) { 
    assert(dynamic_cast<Column*>(to_add));
    return ObjectArray::push(to_add); 
  }

  ColumnArray* clone() { return new ColumnArray(*this); }
  Column* get(size_t index) { return static_cast<Column*>(ObjectArray::get(index)); }
  Column* remove(size_t index) { return static_cast<Column*>(ObjectArray::remove(index)); }
  Column* replace(size_t index, Column* const to_add) { return static_cast<Column*>(ObjectArray::replace(index, to_add)); }
};