#include <sys/wait.h>

#include "../src/application/application.h"
#include "../src/kv_store/key.h"
#include "../src/networks/rendezvous_server.h"
#include "../src/application/word_count_rowers.h"

const char* WORDS_TEXT_FILE = "data/100k.txt";
size_t FILE_WORD_COUNT = 10000;
size_t DIFFERENT_WORD_COUNT = 463;
 
// We took Jan's code (with some function name changes) and put tests throughout them
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
      assert(file_reader->nrows() == FILE_WORD_COUNT);
      assert(file_reader->ncols() == 1);
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
    assert(words->nrows() == FILE_WORD_COUNT);
    p("Node ").p(node_index_).pln(": starting local count...");
    SIMap map;
    Adder add(map);
    words->local_map(add);
    assert(map.size() < FILE_WORD_COUNT);
    assert(map.size() > 0);
    delete words;
    Summer cnt(map);
    assert(map.size() < FILE_WORD_COUNT);
    assert(map.size() > 0);
    DataFrame* test_frame = DataFrame::from_rower(mk_key(node_index_), &kd_, "SI", cnt);
    assert(test_frame->nrows() == map.size());
    Column* test_int_column = test_frame->get_column(1);
    for (size_t ii = 0; ii < map.size(); ii++) {
      assert(test_int_column->get_int(ii) >= 1 && test_int_column->get_int(ii) < FILE_WORD_COUNT);
    }
    assert(map.size() < FILE_WORD_COUNT);
    assert(map.size() > 0);
    delete test_frame;
  }
 
  /** Merge the data frames of all nodes */
  void reduce() {
    if (node_index_ != 0) return;
    pln("Node 0: reducing counts...");
    SIMap map;
    Key* own = mk_key(0);
    merge(kd_.get(own), map);
    assert(map.size() < FILE_WORD_COUNT && map.size() > 0);
    for (size_t i = 1; i < kd_.get_kv()->get_num_other_nodes(); ++i) { // merge other nodes
      Key* ok = mk_key(i);
      merge(kd_.wait_and_get(ok), map);
      delete ok;
    }
    p("Different words: ").pln(map.size());
    assert(map.size() == DIFFERENT_WORD_COUNT);
    delete own;
  }
 
  void merge(DataFrame* df, SIMap& m) {
    Adder add(m);
    df->map(add);
    delete df;
  }
}; // WordcountDemo

void test_word_count() {
  int cpid[3];
  const char* server_ip = "127.0.0.1";
  const char** client_ips = new const char*[3];
  client_ips[0] = "127.0.0.2";
  client_ips[1] = "127.0.0.3";
  client_ips[2] = "127.0.0.4";

  RServer* server = new RServer(server_ip); 

  for (int i = 0; i < 3; i++) {
    if ((cpid[i] = fork())) {
      // parent, do nothing now
    } else {
      // child process
      WordCount* demo = new WordCount(i, client_ips[i], server_ip, WORDS_TEXT_FILE);
      demo->run_();
      delete demo;
      delete server;
      delete[] client_ips;
      exit(0);
    } 
  }

  // In parent process
  server->run_server();
  server->wait_for_shutdown();

  // wait for child to finish
  for (int i = 0; i < 3; i++) {
    int st;
    waitpid(cpid[i], &st, 0);
  }
  delete server;
  delete[] client_ips;

  printf("WordCount application test passed!\n");
}

int main(int argc, char** argv) {
  test_word_count();
  printf("All application tests pass\n");
}