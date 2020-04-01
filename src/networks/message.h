// Made by Kaylin Devchand and Cristian Stransky

#pragma once

#include <stdlib.h>
#include <string.h>
#include "../helpers/string.h"
#include "../helpers/object.h"
#include "../helpers/serial.h"
#include "../helpers/array.h"

int MESSAGE_ID = 0;

// TODO: MESSAGE STILL NEEDS TO BE REFACTORED

enum class MsgKind { Ack, Put, Get, WaitAndGet, Value, Kill, Register, Directory, Complete };
// TODO: A lot of repeated code, make sure to refactor this in the future
class Message : public Object {
    public:

    MsgKind kind_;  // the message kind
    String* sender_; // the index of the sender node
    // TODO: We don't need target ip anymore because we are connecting directly to Nodes,
    // BUT when you do refactor, be very careful and test because of serializing
    String* target_; // the index of the receiver node

    Message(MsgKind kind, String* sender, String* target) {
        kind_ = kind;
        sender_ = sender->clone();
        target_ = target->clone();
    }

    Message(MsgKind kind, Deserializer& deserializer) {
        kind_ = kind;
        sender_ = new String(deserializer);
        target_ = new String(deserializer);
    }

    ~Message() {
        delete sender_;
        delete target_;
    }

    MsgKind get_kind() {
        return kind_;
    }

    String* get_sender() {
        return sender_;
    }

    String* get_target() {
        return target_;
    }

    virtual size_t serial_len() {
        // includes the total serial length at the beginning
        return sizeof(size_t) 
            + sizeof(size_t) 
            + sender_->serial_len() 
            + target_->serial_len();
    }

    virtual char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        return serializer.get_serial();
    }

    virtual void serialize_message_(Serializer& serializer) {
        serializer.serialize_size_t(serializer.get_serial_size());
        serializer.serialize_size_t(static_cast<size_t>(kind_));
        serializer.serialize_object(sender_);
        serializer.serialize_object(target_);
    }

    static Message* deserialize_message(char* buff);
};

 
class Ack : public Message {
    public:
    String* message_;

    Ack(String* sender, String* target, String* message) : Message(MsgKind::Ack, sender, target) {
        message_ = message->clone();
    }

    Ack(Deserializer& deserializer) : Message(MsgKind::Ack, deserializer) {
        message_ = new String(deserializer);
    }

    ~Ack() {
        delete message_;
    }

    String* get_message() {
        return message_;
    }

    size_t serial_len() {
        // includes the total serial length at the beginning
        return Message::serial_len()
            + message_->serial_len();
    }

    char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        serializer.serialize_object(message_);
        return serializer.get_serial();
    }
};

class Kill : public Message {
    public:
    Kill(String* sender, String* target) : Message(MsgKind::Kill, sender, target) {}
    Kill(Deserializer& deserializer) : Message(MsgKind::Kill, deserializer) {}
};

class Complete : public Message {
    public:
    Complete(String* sender, String* target) : Message(MsgKind::Complete, sender, target) {}
    Complete(Deserializer& deserializer) : Message(MsgKind::Complete, deserializer) {}
};

class Register : public Message {
    public:
    size_t node_index_;

    Register(String* sender, String* target, size_t node_index) : Message(MsgKind::Register, sender, target) { 
        node_index_ = node_index;
    }

    Register(Deserializer& deserializer) : Message(MsgKind::Register, deserializer) {
        node_index_ = deserializer.deserialize_size_t();
    }

    size_t get_node_index() {
        return node_index_;
    }

    size_t serial_len() {
        return Message::serial_len() + sizeof(size_t);
    }

    char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        serializer.serialize_size_t(node_index_);
        return serializer.get_serial();
    }
};

class Directory : public Message {
    public:
    StringArray* addresses_;  // owned; strings owned
    IntArray* node_indexes_;

    Directory(String* sender, String* target, StringArray* addresses, IntArray* node_indexes) 
        : Message(MsgKind::Directory, sender, target) {
        addresses_ = addresses->clone();
        node_indexes_ = node_indexes->clone();
    }

    Directory(Deserializer& deserializer) : Message(MsgKind::Directory, deserializer) {
        addresses_ = new StringArray(deserializer);
        node_indexes_ = new IntArray(deserializer);
    }

    ~Directory() {
        delete addresses_;
        delete node_indexes_;
    }

    StringArray* get_addresses() {
        return addresses_;
    }

    IntArray* get_node_indexes() {
        return node_indexes_;
    }

    size_t serial_len() {
        return Message::serial_len() 
            + addresses_->serial_len()
            + node_indexes_->serial_len();
    }

    char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        serializer.serialize_object(addresses_);
        serializer.serialize_object(node_indexes_);
        return serializer.get_serial();
    }
};

class Put : public Message {
    public:
    String* key_name_;
    Serializer* value_;

    Put(String* sender, String* target, String* key_name, Serializer* value) : Message(MsgKind::Put, sender, target) {
        key_name_ = key_name->clone();
        value_ = value->clone();
    }

    Put(Deserializer& deserializer) : Message(MsgKind::Put, deserializer) {
        key_name_ = new String(deserializer);
        size_t len = deserializer.deserialize_size_t();
        char* serial = deserializer.deserialize_char_array(len - 1);
        value_ = new Serializer(serial);
        delete[] serial;
    }

    ~Put() {
        delete key_name_;
        delete value_;
    }

    String* get_key_name() {
        return key_name_;
    }

    Serializer* get_value() {
        return value_;
    }

    /**
     * NOTE: You are getting a NEW character array with this function, so make sure to delete it
     */
    char* get_serial() {
        return value_->get_serial();
    }

    size_t serial_len() {
        return Message::serial_len() 
            + key_name_->serial_len()
            + value_->get_serial_size() + sizeof(size_t);
    }

    char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        serializer.serialize_object(key_name_);
        serializer.serialize_size_t(value_->get_serial_size());
        char* serial = value_->get_serial();
        serializer.serialize_chars(serial, value_->get_serial_size() - 1);
        delete[] serial;
        return serializer.get_serial();
    }
};

class Get : public Message {
    public:
    String* key_name_;

    Get(String* sender, String* target, String* key_name) : Message(MsgKind::Get, sender, target) {
        key_name_ = key_name->clone();
    }

    Get(Deserializer& deserializer) : Message(MsgKind::Get, deserializer) {
        key_name_ = new String(deserializer);
    }

    ~Get() {
        delete key_name_;
    }

    String* get_key_name() {
        return key_name_;
    }

    size_t serial_len() {
        return Message::serial_len() 
            + key_name_->serial_len();
    }

    char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        serializer.serialize_object(key_name_);
        return serializer.get_serial();
    }
};

class WaitAndGet : public Get {
    public:

    WaitAndGet(String* sender, String* target, String* key_name_) : Get(sender, target, key_name_) {
        kind_ = MsgKind::WaitAndGet;
    }

    WaitAndGet(Deserializer& deserializer) : Get(deserializer) {
        kind_ = MsgKind::WaitAndGet;
    }
};

class Value : public Message {
    public:
    Serializer* value_;

    Value(String* sender, String* target, Serializer* value) : Message(MsgKind::Value, sender, target){
        value_ = value->clone();
    }

    Value(Deserializer& deserializer) : Message(MsgKind::Value, deserializer) {
        size_t len = deserializer.deserialize_size_t();
        char* serial = deserializer.deserialize_char_array(len - 1);
        value_ = new Serializer(serial);
        delete[] serial;
    }

    ~Value() {
        delete value_;
    }

    Serializer* get_value() {
        return value_;
    }
    
    /**
     * NOTE: You are getting a NEW character array with this function, so make sure to delete it
     */
    char* get_serial() {
        return value_->get_serial();
    }

    size_t serial_len() {
        return Message::serial_len() 
            + value_->get_serial_size() + sizeof(size_t);
    }

    char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        serializer.serialize_size_t(value_->get_serial_size());
        char* serial = value_->get_serial();
        serializer.serialize_chars(serial, value_->get_serial_size() - 1);
        delete[] serial;
        return serializer.get_serial();
    }
};

Message* Message::deserialize_message(char* buff) {
    Deserializer deserializer(buff);
    deserializer.deserialize_size_t(); // skip serial size
    MsgKind msg_kind = static_cast<MsgKind>(deserializer.deserialize_size_t());
    switch(msg_kind) {
        case MsgKind::Ack:
            return new Ack(deserializer);
        case MsgKind::Directory:
            return new Directory(deserializer);
        case MsgKind::Kill:
            return new Kill(deserializer);
        case MsgKind::Put:
            return new Put(deserializer);
        case MsgKind::Register:
            return new Register(deserializer);
        case MsgKind::Get:
            return new Get(deserializer);
        case MsgKind::WaitAndGet:
            return new WaitAndGet(deserializer);
        case MsgKind::Value:
            return new Value(deserializer);
        case MsgKind::Complete:
            return new Complete(deserializer);
        default:
            assert(0);
    }
}
