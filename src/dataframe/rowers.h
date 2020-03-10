// Made by Kaylin Devchand and Cristian Stransky
#pragma once

#include "dataframe.h"
#include "object.h"
#include <math.h>

/*******************************************************************************
 *  EncryptRower::
 *  A Rower that encrypts each field using textbook RSA.
 */
class EncryptRower : public Rower {
 public:
  DataFrame* df_;
  int public_key_;
  int n_; // only used for ints and floats because message size of bools and chars are smaller

  EncryptRower(DataFrame* df) {
    df_ = df;
    public_key_ = 256;
    n_ = 111409 * 997;
  } 

  ~EncryptRower() {
    
  }

    /** Return a copy of the object; nullptr is considered an error */
  Object* clone() {
    return new EncryptRower(df_);
  }

  /** Always returns true. Encrypts each field by raising the field to the public key then
   *  mod by n. */
  bool accept(Row& r) {
    int sum_ = 0;
    for (size_t i = 0; i < r.width(); i++) {
      switch (r.col_type(i))
      {
      case 'I': {
        int val = r.get_int(i);
        val = (int)pow(val, public_key_) % n_;
        //df_->set(i, r.get_idx(), val);
        break;
      }
      case 'S': {
        String* val = r.get_string(i);
        char* c = val->c_str();
        char e[val->size()];
        // for (int i = 0; i < val->size(); i++) {
        //     e[i] = ((int)pow(c[i], public_key_)) % 255;
        // }
        //delete val;
        //df_->set(i, r.get_idx(), new String(e));
        break;
      }
      case 'B': {
        bool val = r.get_bool(i);
        val = (int)pow(val, public_key_) % 2; // Can't decrypt
        //df_->set(i, r.get_idx(), val);
        break;
      }
      case 'F': {
        float val = r.get_float(i) * 1000000; // Multiply by 1 million so you don't lose stuff
        val = (float)((int)(pow(val, public_key_)) % n_);
        //df_->set(i, r.get_idx(), val);
        break;
      }
      default:
        break;
      }
    }

    return true;
  }

  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  void join_delete(Rower* other) {
    delete other;
  }
 
};

/*******************************************************************************
 *  AddRower::
 *  A Rower to add a constant to every int.
 */
class AddRower : public Rower {
 public:
  DataFrame* df_;

  AddRower(DataFrame* df) {
    df_ = df;
  }

  ~AddRower() {
    
  }

    /** Return a copy of the object; nullptr is considered an error */
  Object* clone() {
    return new AddRower(df_);
  }

  /** Always returns true. Adds the num to each integer. */
  bool accept(Row& r) {
    int sum_ = 0;
    for (size_t i = 0; i < r.width() - 1; i++) {
      switch (r.col_type(i))
      {
      case 'I': {
        int val = r.get_int(i);
        sum_ += val;
        break;
      }
      default:
        break;
      }
    }
    //df_->set((size_t)r.width() - 1, r.get_idx(), sum_);    

    return true;
  }

  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  void join_delete(Rower* other) {
    delete other;
  }
 
};

/*******************************************************************************
 *  AverageRower::
 *  A Rower calculate the average of the first and second column and put the average
 *  in the last column
 */
class AverageRower : public Rower {
 public:
  DataFrame* df_;

  AverageRower(DataFrame* df) {
    df_ = df;
  }

  ~AverageRower() {
    
  }

    /** Return a copy of the object; nullptr is considered an error */
  Object* clone() {
    return new AverageRower(df_);
  }

  /** Always returns true. Adds the num to each integer. */
  bool accept(Row& r) {
    float average = (r.get_int(7) + r.get_int(8)) / 2.0;
    //df_->set(r.width() - 1, r.get_idx(), average);    

    return true;
  }

  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  void join_delete(Rower* other) {
    delete other;
  }
 
};