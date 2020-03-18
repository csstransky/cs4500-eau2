#pragma once

#include "../helpers/string.h"

class Key : public Object {
    public:
    String* key_;
    size_t node_index_;

    Key(String* key, size_t node_index) {
        key_ = key->clone();
        node_index_ = node_index;
    }

    ~Key() {
        delete key_;
    }

    String* get_key() {
        return key_;
    }

    size_t get_node_index() {
        return node_index_;
    }

    /** Subclasses should redefine */
    bool equals(Object  * other) {
        // TODO
    }

    /** Return a copy of the object; nullptr is considered an error */
    Object* clone() {
        // TODO
    }
};