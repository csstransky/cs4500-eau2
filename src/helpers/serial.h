// Made by Kaylin Devchand and Cristian Stransky

#pragma once
#include <stdlib.h> 
#include <string.h> 
#include "object.h"

// TODO: This file could actually use some refactoring to bring down the code base
class Serializer : public Object {
    public:

    size_t serial_index_;
    size_t serial_size_;
    char* serial_;

    Serializer(size_t serial_size) {
        serial_index_ = 0;
        serial_size_ = serial_size;
        serial_ = new char[serial_size];
    }

    Serializer(char* serial, size_t size) {
        serial_size_ = size;
        serial_index_ = serial_size_;
        serial_ = new char[serial_size_];
        memcpy(serial_, serial, serial_size_);
    }

    Serializer(Serializer& from) {
        serial_index_ = from.serial_index_;
        serial_size_ = from.serial_size_;
        serial_ = new char[serial_size_];
        memcpy(serial_, from.serial_, serial_size_);
    }

    ~Serializer() {
        delete[] serial_;
    }

    void serialize_size_t(size_t size_t_value) {
        memcpy(serial_ + serial_index_, &size_t_value, sizeof(size_t));
        serial_index_ += sizeof(size_t);
    }

    void serialize_int(int int_value) {
        memcpy(serial_ + serial_index_, &int_value, sizeof(int));
        serial_index_ += sizeof(int);
    }

    void serialize_double(double double_value) {
        memcpy(serial_ + serial_index_, &double_value, sizeof(double));
        serial_index_ += sizeof(double);
    }

    void serialize_bool(bool bool_value) {
        memcpy(serial_ + serial_index_, &bool_value, sizeof(bool));
        serial_index_ += sizeof(bool);
    }

    void serialize_char(char char_value) {
        memcpy(serial_ + serial_index_, &char_value, sizeof(char));
        serial_index_ += sizeof(char);
    }

    /**
     * 
     * NOTE: size is the number of elements EXCLUDING the null terminator
     */
    void serialize_chars(char* char_array, size_t size) {
        size_t char_array_size = sizeof(char) * (size + 1);
        memcpy(serial_ + serial_index_, char_array, char_array_size);
        serial_index_ += char_array_size;
    }

    void serialize_object(Object* object) {
        size_t object_serial_size = object->serial_len();
        char* object_serial = object->serialize();
        memcpy(serial_ + serial_index_, object_serial, object_serial_size);
        serial_index_ += object_serial_size;
        delete[] object_serial;
    }

    size_t get_serial_size() {
        return serial_size_;
    }

    /**
     * NOTE: You are getting a NEW character array with this function, so make sure to delete it
     */
    char* get_serial() {
        char* new_serial = new char[serial_size_];
        memcpy(new_serial, serial_, serial_size_);
        return new_serial;
    }

    /******* METHODS FROM OBJECT ******/

    bool equals(Object* other) {
        Serializer* other_serial = dynamic_cast<Serializer*>(other);
        return other == this
            || (other_serial != nullptr
                && this->serial_size_ == other_serial->serial_size_
                && this->serial_index_ == other_serial->serial_index_
                && strncmp(this->serial_, other_serial->serial_, serial_size_) == 0);
    }

    Serializer* clone() {
        return new Serializer(*this);
    }
};

// NOTE: When using deserialize methods, it MUST be done in order of the serial given to it, or
// else segfaults will occur
class Deserializer {
    public: 
    char* serial_;
    size_t serial_index_;

    Deserializer(char* serial) {
        serial_ = serial;
        serial_index_ = 0;
    }

    Deserializer(char* serial, size_t serial_index) {
        serial_ = serial;
        serial_index_ = serial_index;
    }

    size_t get_serial_index() {
        return serial_index_;
    }

    void set_serial_index(size_t index) {
        serial_index_ = index;
    }

    size_t deserialize_size_t() {
        size_t size_t_value;
        memcpy(&size_t_value, &serial_[serial_index_], sizeof(size_t));
        serial_index_ += sizeof(size_t);
        return size_t_value;
    }

    int deserialize_int() {
        int int_value;
        memcpy(&int_value, &serial_[serial_index_], sizeof(int));
        serial_index_ += sizeof(int);
        return int_value;
    }

    double deserialize_double() {
        double double_value;
        memcpy(&double_value, &serial_[serial_index_], sizeof(double));
        serial_index_ += sizeof(double);
        return double_value;
    }

    bool deserialize_bool() {
        bool bool_value;
        memcpy(&bool_value, &serial_[serial_index_], sizeof(bool));
        serial_index_ += sizeof(bool);
        return bool_value;
    }

    char deserialize_char() {
        char char_value;
        memcpy(&char_value, &serial_[serial_index_], sizeof(char));
        serial_index_ += sizeof(char);
        return char_value;
    }

    // NOTE: The char_array_size does NOT include the null terminator at the end
    // NOTE: The returned char array must be deleted as well
    char* deserialize_char_array(size_t char_array_size) {
        char* string_chars = new char[char_array_size + 1];
        memcpy(string_chars, &serial_[serial_index_], sizeof(char) * (char_array_size + 1));
        serial_index_ += sizeof(char) * (char_array_size + 1);
        return string_chars;
    }
};

