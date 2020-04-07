#include "application.h"
#include "arguments.h"

// Main test for Milestone 2:
// http://janvitek.org/events/NEU/4500/s20/projects2.html
class Trivial : public Application {
 public:
  Trivial(size_t idx, const char* my_ip, const char* server_ip) : Application(idx, my_ip, server_ip) { }
  void run_() {
    size_t SZ = 1000*1000;
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    Key key("triv",0);
    DataFrame* df = DataFrame::from_array(&key, &kd_, SZ, vals);
    assert(df->get_double(0,1) == 1);
    DataFrame* df2 = kd_.get(&key);
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
    assert(sum==0);
    delete df; 
    delete df2;
    delete[] vals;
  }
};

int main(int argc, const char** argv) {
  const char* client_ip_address = get_input_client_ip_address(argc, argv);
  const char* server_ip_address = get_input_server_ip_address(argc, argv);
  Trivial t_app(0, client_ip_address, server_ip_address);
  t_app.run_();
}