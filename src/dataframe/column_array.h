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

  ColumnArray* clone() { return new ColumnArray(*this); }

  /* Gets a Column at the given index */
  /* Throws an error if not found or out of range or no elements in array*/
  Column* get(size_t index) { return static_cast<Column*>(ObjectArray::get(index)); }

  /* Adds an Column to the end of the Array, returns the new length */
  size_t push(Column* const to_add) { return ObjectArray::push(to_add); }

  /* Removes the last Column of the Array, returns the removed Column */
  /* Throws an error if not found or out of range or no elements in array*/
  Column* pop() { return static_cast<Column*>(ObjectArray::pop()); }

  /* Removes a Column at the given index, returns removed Column */
  /* Throws an error if not found or out of range or no elements in array*/
  Column* remove(size_t index) { return static_cast<Column*>(ObjectArray::remove(index)); }

  /* Replaces a Column at the given index with the given Column, returns the replaced Column */
  /* Throws an error if not found or out of range or no elements in array*/
  Column* replace(size_t index, Column* const to_add) {
    return static_cast<Column*>(ObjectArray::replace(index, to_add));
  }

  static ColumnArray* deserialize(char* serial, KV_Store* kv_store) {
    Deserializer deserializer(serial);
    return deserialize(deserializer, kv_store);
  }

  static ColumnArray* deserialize(Deserializer& deserializer, KV_Store* kv_store) {
    Array* new_array = deserialize_new_array_(deserializer);
    ColumnArray* new_column_array = static_cast<ColumnArray*>(new_array);
    for (size_t ii = 0; ii < new_column_array->count_; ii++) {
      Column* new_object = Column::deserialize(deserializer, kv_store);
      new_column_array->replace(ii, new_object);
      delete new_object;
    }
    return new_column_array;
  }
};