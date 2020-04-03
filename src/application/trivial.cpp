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