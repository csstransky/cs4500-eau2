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
  ColumnArray(char* serial, KV_Store* kv_store) { 
    Deserializer deserializer(serial);
    deserialize_column_array_(deserializer, kv_store);
  }
  ColumnArray(Deserializer& deserializer, KV_Store* kv_store) {
    deserialize_column_array_(deserializer, kv_store);
  }

  void deserialize_column_array_(Deserializer& deserializer, KV_Store* kv_store) {
    deserialize_basic_array_(deserializer);
    for (size_t ii = 0; ii < count_; ii++)
      elements_[ii].o = new Column(deserializer, kv_store);
  }

  ColumnArray* clone() { return new ColumnArray(*this); }

  /* Gets a Column at the given index */
  /* Throws an error if not found or out of range or no elements in array*/
  Column* get(size_t index) { return static_cast<Column*>(ObjectArray::get(index)); }

  /* Adds an Column to the end of the Array, returns the new length */
  size_t push(Object* const to_add) { 
    assert(dynamic_cast<Column*>(to_add));
    return ObjectArray::push(to_add); 
  }

  /* Removes a Column at the given index, returns removed Column */
  /* Throws an error if not found or out of range or no elements in array*/
  Column* remove(size_t index) { return static_cast<Column*>(ObjectArray::remove(index)); }

  /* Replaces a Column at the given index with the given Column, returns the replaced Column */
  /* Throws an error if not found or out of range or no elements in array*/
  Column* replace(size_t index, Column* const to_add) {
    return static_cast<Column*>(ObjectArray::replace(index, to_add));
  }
};