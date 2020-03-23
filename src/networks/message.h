// Made by Kaylin Devchand and Cristian Stransky

#pragma once

#include <stdlib.h>
#include <string.h>
#include "../helpers/string.h"
#include "../helpers/object.h"
#include "../helpers/serial.h"

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
        // Don't need the serial size, so we skip it
        deserializer.deserialize_size_t();
        // Don't need msgkind, so we skip it
        deserializer.deserialize_size_t();
        static String* sender = String::deserialize(deserializer);
        static String* target = String::deserialize(deserializer);
        Ack* new_ack = new Ack(sender, target);
        delete sender;
        delete target;
        return new_ack;
    }
};

class Put : public Message {
    public:
    String* message_;

    Put(String* sender, String* target, String* message) {
        Message::Message_(MsgKind::Put, sender, target); 
        message_ = message->clone();
    }

    ~Put() {
        Message::DMessage_();
        delete message_;
    }

    String* get_message() {
        return message_;
    }

    size_t serial_len() {
        return Message::serial_len() + message_->serial_len();
    }

    char* serialize() {
        size_t serial_size = serial_len();
        Serializer serializer(serial_size);
        serialize_message_(serializer);
        serializer.serialize_object(message_);
        return serializer.get_serial();
    }

    static Put* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static Put* deserialize(Deserializer& deserializer) {
        // Don't need serial size, so we skip it
        deserializer.deserialize_size_t();
        // Don't need msgkind, so we skip it
        deserializer.deserialize_size_t();
        String* sender = String::deserialize(deserializer);
        String* target = String::deserialize(deserializer);
        // Don't need id, so we skip it
        deserializer.deserialize_size_t();
        String* message = String::deserialize(deserializer);
        Put* new_put = new Put(sender, target, message);
        delete sender;
        delete target;
        delete message;
        return new_put;
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
        // Don't need serial size, so we skip it
        deserializer.deserialize_size_t();
        // Don't need msgkind, so we skip it
        deserializer.deserialize_size_t();
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

    // TODO" add local node index here
    Register(String* sender, String* target) {
        Message::Message_(MsgKind::Register, sender, target); 
    }

    ~Register() {
        Message::DMessage_();
    }


    static Register* deserialize(char* serial) {
        Deserializer deserializer(serial);
        return deserialize(deserializer);
    }

    static Register* deserialize(Deserializer& deserializer) {
        // Don't need serial size, so we skip it
        deserializer.deserialize_size_t();
        // Don't need msgkind, so we skip it
        deserializer.deserialize_size_t();
        String* sender = String::deserialize(deserializer);
        String* target = String::deserialize(deserializer);
        Register* new_put = new Register(sender, target);
        delete sender;
        delete target;
        return new_put;
    }
};

class Directory : public Message {
    public:
    String** addresses_;  // owned; strings owned
    size_t address_len_;

    Directory(String* sender, String* target, String** addresses, size_t address_len) {
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
        // Don't need serial size, so we skip it
        deserializer.deserialize_size_t();
        // Don't need msgkind, so we skip it
        deserializer.deserialize_size_t();
        String* sender = String::deserialize(deserializer);
        String* target = String::deserialize(deserializer);
        // Don't need id, so we skip it
        deserializer.deserialize_size_t();

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

MsgKind get_msg_kind(char* serial) {
    Deserializer deserial(serial);
    // Ignore the first serial length size_t
    deserial.deserialize_size_t();
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
