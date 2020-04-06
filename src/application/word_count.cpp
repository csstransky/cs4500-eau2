#include <sys/wait.h>
#include "arguments.h"
#include "word_count.h"


// NOTE: Took this straight from Jan's assignment (with refactoring of some functions)
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
  const char* file_;
 
  WordCount(size_t node_index, const char* my_ip, const char* server_ip, const char* file):
    Application(node_index, my_ip, server_ip), in("data", 0), buf("wc-map-") {
        file_ = file;
     }
 
  /** The master nodes reads the input, then all of the nodes count. */
  void run_() override {
    if (node_index_ == 0) {
      FileReader fr(file_);
      DataFrame* file_reader = DataFrame::from_rower(&in, &kd_, "S", fr);
      delete file_reader;
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
    p("Node ").p(node_index_).pln(": starting local count...");
    SIMap map;
    Adder add(map);
    words->local_map(add);
    delete words;
    Summer cnt(map);
    Key* node_key = mk_key(node_index_);
    delete DataFrame::from_rower(node_key, &kd_, "SI", cnt);
    delete node_key;
  }
 
  /** Merge the data frames of all nodes */
  void reduce() {
    if (node_index_ != 0) return;
    pln("Node 0: reducing counts...");
    SIMap map;
    Key* own = mk_key(0);
    merge(kd_.get(own), map);
    for (size_t i = 1; i < kd_.get_kv()->get_num_other_nodes(); ++i) { // merge other nodes
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

int main(int argc, const char** argv) {
  const char* client_ip_address = get_input_client_ip_address(argc, argv);
  const char* server_ip_address = get_input_server_ip_address(argc, argv);
  const char* text_file = get_input_text_file(argc, argv);
  int node_index = get_input_node_index(argc, argv);
  WordCount app(node_index, client_ip_address, server_ip_address, text_file);
  app.run_();
}