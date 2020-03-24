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
enum class MsgKind { Ack, Put, Get, Value, Kill, Register, Directory };

class Message : public Object {
    public:

    MsgKind kind_;  // the message kind
    String* sender_; // the index of the sender node
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

    Ack(String* sender, String* target) {
        Message::Message_(MsgKind::Ack, sender, target); 
    }

    ~Ack() {
        Message::DMessage_();
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
        Ack* new_ack = new Ack(sender, target);
        delete sender;
        delete target;
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

    // TODO here
    Directory(String* sender, String* target, StringArray* addresses, size_t address_len) {
        Message::Message_(MsgKind::Directory, sender, target); 
        address_len_ = address_len;
        addresses_ = new String*[address_len_];
        for (size_t ii = 0; ii < address_len_; ii++) {
            addresses_[ii] = addresses[ii]->clone();
        }
    }

    ~Directory() {
        Message::DMessage_();
        for (size_t ii = 0; ii < address_len_; ii++) {
            delete addresses_[ii];
        }
        delete[] addresses_;
    }

    String** get_addresses() {
        return addresses_;
    }

    size_t get_num() {
        return address_len_;
    }

    size_t serial_len() {
        size_t addresses_serial_len = 0;
        for (size_t ii = 0; ii < address_len_; ii++) {
            addresses_serial_len += addresses_[ii]->serial_len();
        }
        return Message::serial_len() 
            + sizeof(size_t)
            + addresses_serial_len;
    }

    char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        serializer.serialize_size_t(address_len_);
        for (size_t ii = 0; ii < address_len_; ii++){
            serializer.serialize_object(addresses_[ii]);
        }
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

        size_t address_len = deserializer.deserialize_size_t();
        String* addresses[address_len];
        for (size_t ii = 0; ii < address_len; ii++) {
            addresses[ii] = String::deserialize(deserializer);;
        }
        Directory* new_directory = new Directory(sender, target, addresses, address_len);
        delete sender;
        delete target;
        for (size_t ii = 0; ii < address_len; ii++) {
            delete addresses[ii];
        }
        return new_directory;
    }
};

// TODO: get rid of this
class DataMessage : public Message {
    public:
    String* key_;
    Serializer* value_;
    
    // Private constructor (only visible to sub classes)
    void DataMessage_(MsgKind kind, String* sender, String* target, String* message) {
        Message::Message_(kind, sender, target); 
        message_ = message->clone();
    }

    // Private deconstructor
    void DDataMessage_() {
        Message::DMessage_();
        delete message_;
    }

    String* get_message() {
        return message_;
    }

    virtual size_t serial_len() {
        return Message::serial_len() + message_->serial_len();
    }

    virtual char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_data_message_(serializer);
        return serializer.get_serial();
    }

    void serialize_data_message_(Serializer& serializer) {
        serialize_message_(serializer);
        serializer.serialize_object(message_);
    }
};

class Put : public DataMessage {
    public:
    String* key_name_;
    Serializer* value_;

    Put(String* sender, String* target, String* key, Serializer* value) {
        DataMessage::DataMessage_(MsgKind::Put, sender, target, message); 
    }

    ~Put() {
        DataMessage::DDataMessage_();
    }


    virtual char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        serializer.serialize_chars(value_->get_serial());
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
        String* message = String::deserialize(deserializer);
        Put* new_put = new Put(sender, target, message);
        delete sender;
        delete target;
        delete message;
        return new_put;
    }
};

class Get : public DataMessage {
    public:
    String* key_name_;

    Get(String* sender, String* target, String* key_name) {
        DataMessage::DataMessage_(MsgKind::Get, sender, target, message); 
    }

    ~Get() {
        DataMessage::DDataMessage_();
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
        String* message = String::deserialize(deserializer);
        Get* new_get = new Get(sender, target, message);
        delete sender;
        delete target;
        delete message;
        return new_get;
    }
};

class Value : public DataMessage {
    public:
    Serializer* value_;

    Value(String* sender, String* target, Serializer* value) {
        DataMessage::DataMessage_(MsgKind::Value, sender, target, message); 
    }

    ~Value() {
        DataMessage::DDataMessage_();
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
        String* message = String::deserialize(deserializer);
        Value* new_value = new Value(sender, target, message);
        delete sender;
        delete target;
        delete message;
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
        default:
            assert(0);
    }
}
