#include <sys/wait.h>

#include "application.h"
#include "../kv_store/key.h"
#include "../networks/rendezvous_server.h"
#include "../application/word_count_rowers.h"

const char* DEFAULT_CLIENT_IP = "127.0.0.2";
const char* DEFAULT_SERVER_IP = "127.0.0.1";
const char* DEFAULT_TEXT_FILE = "data/100k.txt";

// TODO: Super rigid input command getter, but it works and we don't need anything too nice
const char* get_input_client_ip_address(int argc, char const *argv[]) {
    if (argc < 2 || strcmp(argv[1], "-ip") != 0) {
        printf("If you wish to choose an IP for the client, use:\n");
        printf("./client -ip <IP address>\n\n");
        return DEFAULT_CLIENT_IP;
    }
    else {
        return argv[2];
    }
}

// TODO: Super rigid input command getter, but it works and we don't need anything too nice
const char* get_input_server_ip_address(int argc, char const *argv[]) {
    if (argc < 4 || strcmp(argv[3], "-s") != 0) {
        printf("If you wish to choose an IP for the client AND the server, use:\n");
        printf("./client -ip <IP address> -s <Server IP address>\n\n");
        return DEFAULT_SERVER_IP;
    }
    else {
        return argv[4];
    }
}

// TODO: Super rigid input command getter, but it works and we don't need anything too nice
const char* get_input_text_file(int argc, char const *argv[]) {
    if (argc < 6 || strcmp(argv[5], "-o") != 0) {
        printf("If you wish to choose an IP for the client AND the server AND the text file, use:\n");
        printf("./client -ip <IP address> -s <Server IP address> -o <text file>\n\n");
        return DEFAULT_TEXT_FILE;
    }
    else {
        return argv[6];
    }
}

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
  int node_index = atoi(argv[7]);
  WordCount app(node_index, client_ip_address, server_ip_address, text_file);
  app.run_();
}