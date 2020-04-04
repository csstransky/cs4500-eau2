#include "../src/application/application.h"
#include "../src/kv_store/key.h"

class FileReader : public Rower {
public:
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
 
    static const size_t BUFSIZE = 1024;
 
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
            if (!isspace(buf_[i_]))
                return;
            // otherwise skip it
            ++i_;
        }
    }

    void join_delete(Rower* other) { delete other; }
 
    char * buf_;
    size_t end_ = 0;
    size_t i_ = 0;
    FILE * file_;
};
 
 
/****************************************************************************/
class Adder : public Rower {
public:
  SIMap& map_;  // String to Num map;  Num holds an int
 
  Adder(SIMap& map) : map_(map)  {}
 
  bool accept(Row& r) override {
    String* word = r.get_string(0);
    assert(word != nullptr);
    Num* num = map_.get(word);
    if (num) {
        num->v++;
    } else {
        Num n;
        map_.put(word, &n);
    }
    return false;
  }

  void join_delete(Rower* other) { delete other; }
};
 
/***************************************************************************/
class Summer : public Rower {
public:
  SIMap& map_;
  size_t i = 0;
  StringArray* keys;
 
  Summer(SIMap& map) : map_(map) {
      keys = map_.keySet();
  }

  ~Summer() {
      delete keys;
  }
 
  bool accept(Row& r) {
      String* key = keys->get(i);
      size_t value = map_.get(key)->v;
      r.set(0, key);
      r.set(1, (int) value);

      return i == keys->length();
  }

  void join_delete(Rower* other) { delete other; }
 };
 
/****************************************************************************
 * Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 **********************************************************author: pmaj ****/
class WordCount: public Application {
public:
  static const size_t BUFSIZE = 1024;
  Key in;
  KeyBuff kbuf;
  SIMap all;
  const char* file;
 
  WordCount(size_t node_index, const char* my_ip, const char* server_ip, const char* file):
    Application(node_index, my_ip, server_ip), in("data", 0), kbuf(new Key("wc-map-",0)) {
        file = file;
     }
 
  /** The master nodes reads the input, then all of the nodes count. */
  void run_() override {
    if (index == 0) {
      FileReader fr(file);
      delete DataFrame::fromVisitor(&in, &kd_, "S", fr);
    }
    local_count();
    reduce();
  }
 
  /** Returns a key for given node.  These keys are homed on master node
   *  which then joins them one by one. */
  Key* mk_key(size_t idx) {
      Key * k = kbuf.c(idx).get();
      LOG("Created key " << k->c_str());
      return k;
  }
 
  /** Compute word counts on the local node and build a data frame. */
  void local_count() {
    DataFrame* words = (kd_.wait_and_get(&in));
    p("Node ").p(index).pln(": starting local count...");
    SIMap map;
    Adder add(map);
    words->local_map(add);
    delete words;
    Summer cnt(map);
    delete DataFrame::fromVisitor(mk_key(index), &kd_, "SI", cnt);
  }
 
  /** Merge the data frames of all nodes */
  void reduce() {
    if (index != 0) return;
    pln("Node 0: reducing counts...");
    SIMap map;
    Key* own = mk_key(0);
    merge(kd_.get(*own), map);
    for (size_t i = 1; i < arg.num_nodes; ++i) { // merge other nodes
      Key* ok = mk_key(i);
      merge(kd_.waitAndGet(*ok), map);
      delete ok;
    }
    p("Different words: ").pln(map.size());
    delete own;
  }
 
  void merge(DataFrame* df, SIMap& m) {
    Adder add(m);
    df->map(add);
    delete df;
  }
}; // WordcountDemo