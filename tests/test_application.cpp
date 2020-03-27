#include "../src/application/application.h"

class Trivial : public Application {
 public:
  Trivial(size_t idx) : Application(idx) { }
  void run_() {
    size_t SZ = 1000 * 1000;
    float* vals = new float[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) {
        vals[i] = i;
        sum = sum + i;
    } 

    Key key("triv",0);
    DataFrame* df = DataFrame::from_array(&key, &kd_, SZ, vals);

    for (size_t i = 0; i < SZ; i++) {
        assert(df->get_float(0,i) == (float)i);
    }
    
    DataFrame* df2 = kd_.get(&key);

    for (size_t i = 0; i < SZ; ++i) {
        float val = df2->get_float(0,i);
        assert(val == (float)i);
        sum = sum - i;
    } 

    assert(sum==0);
    delete df; 
    delete df2;
    delete[] vals;
  }
};

int main(int argc, char** argv) {
  Trivial t_app(0);
  t_app.run_();
}