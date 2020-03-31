#include "application.h"

const char* DEFAULT_CLIENT_IP = "127.0.0.2";
const char* DEFAULT_SERVER_IP = "127.0.0.1";

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

int main(int argc, const char** argv) {
  const char* client_ip_address = get_input_client_ip_address(argc, argv);
  const char* server_ip_address = get_input_server_ip_address(argc, argv);
  int node_index = atoi(argv[5]);
  Demo app(node_index, client_ip_address, server_ip_address);
  app.run_();
}