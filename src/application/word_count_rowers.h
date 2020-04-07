// author: pmaj

#include "../dataframe/dataframe.h"

/** NOTE: file has to end in newline for the last word to be correct **/
class FileReader : public Rower {
  public:
  static const size_t BUFSIZE = 1024;
  char * buf_;
  size_t end_ = 0;
  size_t i_ = 0;
  FILE * file_;

  /** Creates the reader and opens the file for reading.  */
  FileReader(const char* file) {
    file_ = fopen(file, "r");
    if (file_ == nullptr) {
      printf("Cannot open file %s\n", file);
      assert(0);
    }
    buf_ = new char[BUFSIZE + 1]; //  null terminator
    fillBuffer_();
    skipWhitespace_();
  }

  ~FileReader() {
    fclose(file_);
    delete[] buf_;
  }
  
  /** Reads next word and stores it in the row. Actually read the word.
    While reading the word, we may have to re-fill the buffer  */
  /** Returns true when there are no more words to read.  There is nothing
   more to read if we are at the end of the buffer and the file has
  all been read.     */
  bool accept(Row & r) override {
    assert(i_ < end_);
    assert(! isspace(buf_[i_]));
    size_t wStart = i_;
    while (true) {
      if (i_ == end_) {
        if (feof(file_)) { ++i_;  break; }
        i_ = wStart;
        wStart = 0;
        fillBuffer_();
      }
      if (isspace(buf_[i_]))  break;
      ++i_;
    }
    buf_[i_] = 0;
    String word(buf_ + wStart, i_ - wStart);
    r.set(0, &word);
    ++i_;
    skipWhitespace_();

    return (i_ >= end_) && feof(file_);
  }

  /** Reads more data from the file. */
  void fillBuffer_() {
    size_t start = 0;
    // compact unprocessed stream
    if (i_ != end_) {
      start = end_ - i_;
      memcpy(buf_, buf_ + i_, start);
    }
    // read more contents
    end_ = start + fread(buf_+start, sizeof(char), BUFSIZE - start, file_);
    i_ = start;
  }

  /** Skips spaces.  Note that this may need to fill the buffer if the
      last character of the buffer is space itself.  */
  void skipWhitespace_() {
    while (true) {
      if (i_ == end_) {
        if (feof(file_)) return;
        fillBuffer_();
      }
      // if the current character is not whitespace, we are done
      if (!isspace(buf_[i_])) return;
      // otherwise skip it
      ++i_;
    }
  }

  void join_delete(Rower* other) { delete other; }

};

class Summer : public Rower {
public:
  SIMap& map_;
  size_t index_ = 0;
  StringArray* keys_;
 
  Summer(SIMap& map) : map_(map) {
    keys_ = map_.key_set();
  }

  ~Summer() {
    delete keys_;
  }
 
  bool accept(Row& r) {
    String* key = keys_->get(index_);
    size_t value = map_.get(key)->value;
    r.set(0, key);
    r.set(1, (int) value);
    index_++;
    return index_ == keys_->length();
  }

  void join_delete(Rower* other) { delete other; }
 };

class Adder : public Rower {
public:
  SIMap& map_;  // String to Num map;  Num holds an int
 
  Adder(SIMap& map) : map_(map)  {}
 
  bool accept(Row& r) override {
    String* word = r.get_string(0);
    assert(word != nullptr);
    Num* num = map_.get(word);
    if (num) {
        num->value++;
    } else {
        Num n(1);
        map_.put(word, &n);
    }
    return false;
  }

  void join_delete(Rower* other) { delete other; }
};