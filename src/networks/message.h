// Made by Kaylin Devchand and Cristian Stransky

#pragma once

#include <stdlib.h>
#include <string.h>
#include "../helpers/string.h"
#include "../helpers/object.h"
#include "../helpers/serial.h"
#include "../helpers/array.h"

int MESSAGE_ID = 0;

// TODO: Add Get and Value messages
enum class MsgKind { Ack, Put, Get, WaitAndGet, Value, Kill, Register, Directory, Complete };

class Message : public Object {
    public:

    MsgKind kind_;  // the message kind
    String* sender_; // the index of the sender node
    // TODO: We don't need target ip anymore because we are connecting directly to Nodes,
    // BUT when you do refactor, be very careful and test because of serializing
    String* target_; // the index of the receiver node
    size_t id_;     // an id t unique within the node

    // Private constructor (only visible to sub classes)
    void Message_(MsgKind kind, String* sender, String* target) {
        kind_ = kind;
        sender_ = sender->clone();
        target_ = target->clone();
        id_ = MESSAGE_ID;
        MESSAGE_ID++;
    }

    // Private constructor (only visible to sub classes)
    void Message_(MsgKind kind, String* sender, String* target, size_t id) {
        kind_ = kind;
        sender_ = sender->clone();
        target_ = target->clone();
        id_ = id;
    }

    void DMessage_() {
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
            + target_->serial_len() 
            + sizeof(size_t);
    }

    virtual char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        return serializer.get_serial();
    }

    void serialize_message_(Serializer& serializer) {
        serializer.serialize_size_t(serializer.get_serial_size());
        serializer.serialize_size_t(static_cast<size_t>(kind_));
        serializer.serialize_object(sender_);
        serializer.serialize_object(target_);
        serializer.serialize_size_t(id_);
    }
};

 
class Ack : public Message {
    public:
    String* message_;

    Ack(String* sender, String* target, String* message) {
        Message::Message_(MsgKind::Ack, sender, target); 
        message_ = message->clone();
    }

    ~Ack() {
        Message::DMessage_();
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

    static Ack* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static Ack* deserialize(Deserializer& deserializer) {
        deserializer.deserialize_size_t(); // skip serial size
        deserializer.deserialize_size_t(); // skip kind_
        static String* sender = String::deserialize(deserializer);
        static String* target = String::deserialize(deserializer);
        deserializer.deserialize_size_t(); // skip id_
        String* message = String::deserialize(deserializer);
        Ack* new_ack = new Ack(sender, target, message);
        delete sender;
        delete target;
        delete message;
        return new_ack;
    }
};

class Kill : public Message {
    public:

    Kill(String* sender, String* target) {
        Message::Message_(MsgKind::Kill, sender, target); 
    }

    ~Kill() {
        Message::DMessage_();
    }

    static Kill* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static Kill* deserialize(Deserializer& deserializer) {
        deserializer.deserialize_size_t(); // skip serial size
        deserializer.deserialize_size_t(); // skip kind_
        String* sender = String::deserialize(deserializer);
        String* target = String::deserialize(deserializer);
        Kill* new_put = new Kill(sender, target);
        delete sender; 
        delete target;
        return new_put;
    }
};

class Complete : public Message {
    public:

    Complete(String* sender, String* target) {
        Message::Message_(MsgKind::Complete, sender, target); 
    }

    ~Complete() {
        Message::DMessage_();
    }

    static Complete* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static Complete* deserialize(Deserializer& deserializer) {
        deserializer.deserialize_size_t(); // skip serial size
        deserializer.deserialize_size_t(); // skip kind_
        String* sender = String::deserialize(deserializer);
        String* target = String::deserialize(deserializer);
        Complete* new_put = new Complete(sender, target);
        delete sender; 
        delete target;
        return new_put;
    }
};

class Register : public Message {
    public:
    size_t node_index_;

    Register(String* sender, String* target, size_t node_index) {
        Message::Message_(MsgKind::Register, sender, target); 
        node_index_ = node_index;
    }

    ~Register() {
        Message::DMessage_();
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

    static Register* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static Register* deserialize(Deserializer& deserializer) {
        deserializer.deserialize_size_t(); // skip serial size
        deserializer.deserialize_size_t(); // skip kind_
        String* sender = String::deserialize(deserializer);
        String* target = String::deserialize(deserializer);
        deserializer.deserialize_size_t(); // skip id_
        size_t node_index = deserializer.deserialize_size_t();
        Register* new_put = new Register(sender, target, node_index);
        delete sender;
        delete target;
        return new_put;
    }
};

class Directory : public Message {
    public:
    StringArray* addresses_;  // owned; strings owned
    IntArray* node_indexes_;

    Directory(String* sender, String* target, StringArray* addresses, IntArray* node_indexes) {
        Message::Message_(MsgKind::Directory, sender, target); 
        addresses_ = addresses->clone();
        node_indexes_ = node_indexes->clone();
    }

    ~Directory() {
        Message::DMessage_();
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

    static Directory* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static Directory* deserialize(Deserializer& deserializer) {
        deserializer.deserialize_size_t(); // skip serial size
        deserializer.deserialize_size_t(); // skip kind_
        String* sender = String::deserialize(deserializer);
        String* target = String::deserialize(deserializer);
        deserializer.deserialize_size_t(); // skip id_
        StringArray* addresses = StringArray::deserialize(deserializer);
        IntArray* node_indexes = IntArray::deserialize(deserializer);
        Directory* new_directory = new Directory(sender, target, addresses, node_indexes);
        delete sender;
        delete target;
        delete addresses;
        delete node_indexes;
        return new_directory;
    }
};

class Put : public Message {
    public:
    String* key_name_;
    Serializer* value_;

    Put(String* sender, String* target, String* key_name, Serializer* value) {
        Message::Message_(MsgKind::Put, sender, target);
        key_name_ = key_name->clone();
        value_ = value->clone();
    }

    ~Put() {
        Message::DMessage_();
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
            + value_->serial_len();
    }

    char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        serializer.serialize_object(key_name_);
        serializer.serialize_object(value_);
        return serializer.get_serial();
    }

    static Put* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static Put* deserialize(Deserializer& deserializer) {
        deserializer.deserialize_size_t(); // skip serial size
        deserializer.deserialize_size_t(); // skip kind_
        String* sender = String::deserialize(deserializer);
        String* target = String::deserialize(deserializer);
        deserializer.deserialize_size_t(); // skip id_
        String* key_name = String::deserialize(deserializer);
        Serializer* value = Serializer::deserialize(deserializer);

        Put* new_put = new Put(sender, target, key_name, value);
        delete sender;
        delete target;
        delete key_name;
        delete value;
        return new_put;
    }
};

class Get : public Message {
    public:
    String* key_name_;

    Get(String* sender, String* target, String* key_name) {
        Message::Message_(MsgKind::Get, sender, target); 
        key_name_ = key_name->clone();
    }

    ~Get() {
        Message::DMessage_();
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

    static Get* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static Get* deserialize(Deserializer& deserializer) {
        deserializer.deserialize_size_t(); // skip serial size
        deserializer.deserialize_size_t(); // skip kind_
        String* sender = String::deserialize(deserializer);
        String* target = String::deserialize(deserializer);
        deserializer.deserialize_size_t(); // skip id_
        String* key_name = String::deserialize(deserializer);
        Get* new_get = new Get(sender, target, key_name);
        delete sender;
        delete target;
        delete key_name;
        return new_get;
    }
};

class WaitAndGet : public Get {
    public:

    WaitAndGet(String* sender, String* target, String* key_name_) : Get(sender, target, key_name_) {
        kind_ = MsgKind::WaitAndGet;
    }

    static WaitAndGet* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static WaitAndGet* deserialize(Deserializer& deserializer) {
        deserializer.deserialize_size_t(); // skip serial size
        deserializer.deserialize_size_t(); // skip kind_
        String* sender = String::deserialize(deserializer);
        String* target = String::deserialize(deserializer);
        deserializer.deserialize_size_t(); // skip id_
        String* key_name = String::deserialize(deserializer);
        WaitAndGet* new_get = new WaitAndGet(sender, target, key_name);
        delete sender;
        delete target;
        delete key_name;
        return new_get;
    }

};

class Value : public Message {
    public:
    Serializer* value_;

    Value(String* sender, String* target, Serializer* value) {
        Message::Message_(MsgKind::Value, sender, target); 
        value_ = value->clone();
    }

    ~Value() {
        Message::DMessage_();
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
            + value_->serial_len();
    }

    char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        serializer.serialize_object(value_);
        return serializer.get_serial();
    }

    static Value* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static Value* deserialize(Deserializer& deserializer) {
        deserializer.deserialize_size_t(); // skip serial size
        deserializer.deserialize_size_t(); // skip kind_
        String* sender = String::deserialize(deserializer);
        String* target = String::deserialize(deserializer);
        deserializer.deserialize_size_t(); // skip id_
        Serializer* value = Serializer::deserialize(deserializer);

        Value* new_value = new Value(sender, target, value);
        delete sender;
        delete target;
        delete value;
        return new_value;
    }
};

MsgKind get_msg_kind(char* serial) {
    Deserializer deserial(serial);
    deserial.deserialize_size_t(); // skip serial size
    return static_cast<MsgKind>(deserial.deserialize_size_t());
}

Message* deserialize_message(char* buff) {
    MsgKind msg_kind = get_msg_kind(buff);
    switch(msg_kind) {
        case MsgKind::Ack:
            return Ack::deserialize(buff);
        case MsgKind::Directory:
            return Directory::deserialize(buff);
        case MsgKind::Kill:
            return Kill::deserialize(buff);
        case MsgKind::Put:
            return Put::deserialize(buff);
        case MsgKind::Register:
            return Register::deserialize(buff);
        case MsgKind::Get:
            return Get::deserialize(buff);
        case MsgKind::WaitAndGet:
            return WaitAndGet::deserialize(buff);
        case MsgKind::Value:
            return Value::deserialize(buff);
        case MsgKind::Complete:
            return Complete::deserialize(buff);
        default:
            assert(0);
    }
}
