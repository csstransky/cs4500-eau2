#include <sys/wait.h>

#include "../src/application/application.h"
#include "../src/networks/rendezvous_server.h"

class Demo : public Application {
public:
  Key main;
  Key verify;
  Key check;
 
  Demo(size_t idx, const char* my_ip, const char* server_ip):
   Application(idx, my_ip, server_ip), main("main", 0), verify("verif", 0), check("ck", 0) {}
 
  void run_() override {
    switch(this_node()) {
    case 0:   producer();     break;
    case 1:   counter();      break;
    case 2:   summarizer();
   }
  }
 
  void producer() {
    size_t SZ = 100*1000;
    double* vals = new double[SZ];
    int sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    DataFrame* df1= DataFrame::from_array(&main, &kd_, SZ, vals);
    DataFrame* df2 = DataFrame::from_scalar(&check, &kd_, sum);

    delete[] vals;
    delete df1;
    delete df2;
  }
 
  void counter() {
    DataFrame* v = kd_.wait_and_get(&main);
    int sum = 0;
    for (size_t i = 0; i < 100*1000; ++i) sum += v->get_double(0,i);
    p("The sum is  ").pln(sum);
    DataFrame* df = DataFrame::from_scalar(&verify, &kd_, sum);

    delete v;
    delete df;
  }
 
  void summarizer() {
    DataFrame* result = kd_.wait_and_get(&verify);
    DataFrame* expected = kd_.wait_and_get(&check);
    pln(expected->get_int(0,0)==result->get_int(0,0) ? "SUCCESS":"FAILURE");

    delete result;
    delete expected;
  }
};

class Trivial : public Application {
 public:
  Trivial(size_t idx, const char* my_ip, const char* server_ip) : Application(idx, my_ip, server_ip) { }
  void run_() {
    size_t SZ = 1000 * 1000;
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) {
        vals[i] = i;
        sum = sum + i;
    } 

    Key key("triv",0);
    DataFrame* df = DataFrame::from_array(&key, &kd_, SZ, vals);

    for (size_t i = 0; i < SZ; i++) {
        assert(df->get_double(0,i) == (double)i);
    }
    
    DataFrame* df2 = kd_.get(&key);

    for (size_t i = 0; i < SZ; ++i) {
        double val = df2->get_double(0,i);
        assert(val == (double)i);
        sum = sum - i;
    } 

    assert(sum==0);
    delete df; 
    delete df2;
    delete[] vals;
  }
};

void test_trivial() {
  const char* server_ip = "127.0.0.1";
  const char* my_ip = "127.0.0.2";
  int cpid;
  // Fork to create another process
  if ((cpid = fork())) {
      // In parent process

      // Start server
      RServer* server = new RServer(server_ip); 
      server->run_server();
      server->wait_for_shutdown();

      // wait for child to finish
      int st;
      waitpid(cpid, &st, 0);
      delete server;
  } else {
      // In child process

      // sleep .5s
      sleep(0.5);

      // start node
      Trivial* t_app = new Trivial(0, my_ip, server_ip);
      t_app->run_();

      delete t_app;
      // exit
      exit(0);
  }

  printf("Trivial test passed!\n");

}

void test_demo() {
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
      Demo* demo = new Demo(i, client_ips[i], server_ip);
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

  printf("Demo application test passed!\n");
}

int main(int argc, char** argv) {
  //test_trivial();
  test_demo();
  printf("All application tests pass\n");
}