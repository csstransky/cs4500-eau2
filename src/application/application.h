#include "../kv_store/kd_store.h"

class Application : public Object {
    public:
    KD_Store kd_;
    size_t node_index_;

    Application(size_t node_index) : kd_(node_index) {
        node_index_ = node_index;
    }

    virtual void run_() = 0;

    size_t this_node() {
        return node_index_;
    }

};