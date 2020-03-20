#include "application.h"

class Trivial : public Application {
 public:
  Trivial(size_t idx) : Application(idx) { }
  void run_() {
    size_t SZ = 1000*1000;
    float* vals = new float[SZ];
    float sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    String triv("triv");
    Key key(&triv,0);
    DataFrame* df = DataFrame::from_array(&key, kd_, SZ, vals);
    assert(df->get_float(0,1) == 1);
    DataFrame* df2 = kd_->get(&key);
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_float(0,i);
    assert(sum==0);
    delete df; 
    delete df2;
  }
};

int main(int argc, char** argv) {
  Trivial t_app(0);
}