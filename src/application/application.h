#include "kd_store.h"

class Application : public Object {
    public:
    KD_Store* kd_;
    size_t node_index_;

    Application(size_t node_index) {
        node_index_ = node_index;
        kd_ = new KD_Store(node_index);
        run_();
    }

    virtual void run_() {

    }

    size_t this_node() {
        return node_index_;
    }

};