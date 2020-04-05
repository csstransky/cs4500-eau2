#include "../src/application/application.h"
#include "../src/kv_store/key.h"
#include "../src/application/word_count_rowers.h"
 
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
  String buf;
  SIMap all;
  const char* file;
 
  WordCount(size_t node_index, const char* my_ip, const char* server_ip, const char* file):
    Application(node_index, my_ip, server_ip), in("data", 0), buf("wc-map-") {
        file = file;
     }
 
  /** The master nodes reads the input, then all of the nodes count. */
  void run_() override {
    if (index == 0) {
      FileReader fr(file);
      delete DataFrame::from_rower(&in, &kd_, "S", fr);
    }
    local_count();
    reduce();
  }
 
  /** Returns a key for given node.  These keys are homed on master node
   *  which then joins them one by one. */
  Key* mk_key(size_t idx) {
    String s(buf);
    s.concat(idx);
    Key * k = new Key(&s, 0);
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
    delete DataFrame::from_rower(mk_key(node_index_), &kd_, "SI", cnt);
  }
 
  /** Merge the data frames of all nodes */
  void reduce() {
    if (index != 0) return;
    pln("Node 0: reducing counts...");
    SIMap map;
    Key* own = mk_key(0);
    merge(kd_.get(own), map);
    for (size_t i = 1; i < kd_.kv_->get_num_other_nodes(); ++i) { // merge other nodes
      Key* ok = mk_key(i);
      merge(kd_.wait_and_get(ok), map);
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