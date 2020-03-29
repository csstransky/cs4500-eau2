#include "../kv_store/kd_store.h"

class Application : public Object {
    public:
    KD_Store kd_;
    size_t node_index_;

    Application(size_t node_index, const char* my_ip, const char* server_ip) : kd_(node_index, my_ip, server_ip) {
        node_index_ = node_index;
        // sleep a little to wait for all nodes to be running
        sleep(2);
    }

    ~Application() {
        kd_.application_complete();
    }

    virtual void run_() = 0;

    size_t this_node() {
        return node_index_;
    }

};