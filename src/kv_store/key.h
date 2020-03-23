#pragma once

#include "../helpers/string.h"

class Key : public Object {
    public:
    String* key_;
    size_t node_index_;

    Key(const char* key, size_t node_index) {
        key_ = new String(key);
        node_index_ = node_index;
    }

    Key(String* key, size_t node_index) {
        key_ = key->clone();
        node_index_ = node_index;
    }

    Key(Key& from) : Object(from) {
        key_ = from.key_->clone();
        node_index_ = from.node_index_;
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

    bool equals(Object* other) {
        if (other == this) return true;
        Key* other_key = dynamic_cast<Key*>(other);
        return other_key != nullptr 
            && this->key_->equals(other_key->key_)
            && this->node_index_ == other_key->node_index_;
    }

    Key* clone() {
        return new Key(*this);
    }

    size_t serial_len() {
        // Includes the seial length, size of the key string, and size of node index
        return sizeof(size_t) + key_->serial_len() + sizeof(size_t);
    }

    char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serializer.serialize_size_t(serial_size);
        serializer.serialize_object(key_);
        serializer.serialize_size_t(node_index_);
        return serializer.get_serial();
    }

    static Key* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static Key* deserialize(Deserializer& deserializer) {
        deserializer.deserialize_size_t();
        String* key_string = String::deserialize(deserializer);
        size_t node_index = deserializer.deserialize_size_t();
        Key* new_key = new Key(key_string, node_index);
        delete key_string;
        return new_key;
    }
};