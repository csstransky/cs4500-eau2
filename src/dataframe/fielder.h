#pragma once
#include "column_array.h"

// TODO: This can actually be deleted (as per Jan's request)
/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object {
public:

  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r) = 0;
 
  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b) = 0;

  virtual void accept(double f) = 0;

  virtual void accept(int i) = 0;

  virtual void accept(String* s) = 0;
 
  /** Called when all fields have been seen. */
  virtual void done() = 0;
  
};

// TODO: Remove this as it will NOT be viable in the future with 10GB of data
/*****************************************************************************
 * PrinterFielder::
 * Prints out each field in the row.
 */
class PrinterFielder : public Fielder {
public:

  PrinterFielder() {

  }

  ~PrinterFielder() {
    
  }
 
  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  void start(size_t r) {

  }
 
  /** Called for fields of the argument's type with the value of the field. */
  void accept(bool b) {
    printf("<%d> ", b);
  }

  void accept(double f) {
    printf("<%f> ", f);
  }

  void accept(int i) {
    printf("<%d> ", i);
  }
  
  void accept(String* s) {
    if (s == nullptr) {
      printf("<> ");
    }
    else {
      printf("<%s> ", s->c_str());
    }
  }
 
  /** Called when all fields have been seen. */
  void done() {
    printf("\n");
  }
};
