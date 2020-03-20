#pragma once

#include "../dataframe/column.h"

/**
 * An Array class to which Columns can be added to and removed from.
 * author: kaylindevchand & csstransky */
class ColumnArray : public ObjectArray {
public:
  /** CONSTRUCTORS & DESTRUCTORS **/

  /* Creates an Array of desired size */
  ColumnArray(const size_t size) : ObjectArray(size) {
    
  }

  /* Copies the contents of an already existing Array */
  ColumnArray(ColumnArray* arr) : ObjectArray(arr) {
    
  }

  /* Clears Array from memory */
  ~ColumnArray() {

  }

  /** ARRAY METHODS **/

  ColumnArray* clone() {
    return new ColumnArray(this);
  }

  /* Adds an ColumnArray to existing contents */
  void concat(ColumnArray* const arr) {
    ObjectArray::concat(arr);
  }

  /* Gets a Column at the given index */
  /* Throws an error if not found or out of range or no elements in array*/
  Column* get(size_t index) {
    return dynamic_cast<Column*>(ObjectArray::get(index));
  }

  /* Removes the last Column of the Array, returns the removed Column */
  /* Throws an error if not found or out of range or no elements in array*/
  Column* pop() {
    return dynamic_cast<Column*>(ObjectArray::pop());
  }

  /* Adds an Column to the end of the Array, returns the new length */
  size_t push(Column* const to_add) {
    return ObjectArray::push(to_add);
  }

  /* Removes a Column at the given index, returns removed Column */
  /* Throws an error if not found or out of range or no elements in array*/
  Column* remove(size_t index) {
    return dynamic_cast<Column*>(ObjectArray::remove(index));
  }

  /* Replaces a Column at the given index with the given Column, returns the replaced Column */
  /* Throws an error if not found or out of range or no elements in array*/
  Column* replace(size_t index, Column* const to_add) {
    return dynamic_cast<Column*>(ObjectArray::replace(index, to_add));
  }

  static ColumnArray* deserialize(char* serial, KV_Store* kv_store) {
    Deserializer deserializer(serial);
    return deserialize(deserializer, kv_store);
  }

  static ColumnArray* deserialize(Deserializer& deserializer, KV_Store* kv_store) {
    deserializer.deserialize_size_t();
    size_t size = deserializer.deserialize_size_t();
    size_t count = deserializer.deserialize_size_t();
    ColumnArray* new_array = new ColumnArray(size);
    for (size_t ii = 0; ii < count; ii++) {
      // TODO: We'll have to add KV stores somehow in the future, nullptr won't cut it
      Column* new_object = Column::deserialize(deserializer, kv_store);
      new_array->push(new_object);
      delete new_object;
    }
    return new_array;
  }
};