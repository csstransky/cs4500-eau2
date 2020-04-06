#include "application.h"
#include "arguments.h"

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
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    DataFrame* df1= DataFrame::from_array(&main, &kd_, SZ, vals);
    DataFrame* df2 = DataFrame::from_scalar(&check, &kd_, sum);

    delete[] vals;
    delete df1;
    delete df2;
  }
 
  void counter() {
    DataFrame* v = kd_.wait_and_get(&main);
    double sum = 0;
    for (size_t i = 0; i < 100*1000; ++i) sum += v->get_double(0,i);
    p("The sum is  ").pln(sum);
    DataFrame* df = DataFrame::from_scalar(&verify, &kd_, sum);

    delete v;
    delete df;
  }
 
  void summarizer() {
    DataFrame* result = kd_.wait_and_get(&verify);
    DataFrame* expected = kd_.wait_and_get(&check);
    pln(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");

    delete result;
    delete expected;
  }
};

int main(int argc, const char** argv) {
  const char* client_ip_address = get_input_client_ip_address(argc, argv);
  const char* server_ip_address = get_input_server_ip_address(argc, argv);
  int node_index = atoi(argv[5]);
  Demo app(node_index, client_ip_address, server_ip_address);
  app.run_();
}