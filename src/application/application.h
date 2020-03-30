#include "../kv_store/kd_store.h"

/**
 * An Application runs its own section of code as determined in the run_() function by node_index.
 * For example, node 0 may producer() a DataFrame, while node 1 will counter() all the elements.
 * 
 * NOTE: This is a parent class that MUST be used by children classes (such as Demo).
 * Authors: Kaylin Devchand & Cristian Stransky
 */
class Application : public Object {
    public:
    KD_Store kd_;
    size_t node_index_;

    Application(size_t node_index, const char* my_ip, const char* server_ip) : kd_(node_index, my_ip, server_ip) {
        node_index_ = node_index;
        // TODO: Maybe we can get rid of this and allow reconnecting in the future
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