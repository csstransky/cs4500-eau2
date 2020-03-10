// Made by Kaylin Devchand and Cristian Stransky

#include <stdio.h> 
#include <sys/socket.h> 
#include <arpa/inet.h> 
#include <unistd.h> 
#include <string.h> 
#include "../src/message.h"
#include "../src/array.h"


void test_string() {
   String* string1 = new String("hello there");
   assert(string1->size() == 11);
   assert(strcmp(string1->c_str(), "hello there") == 0);

   char* string_serial = string1->serialize();
   String* string2 = String::deserialize(string_serial);

   assert(string2->size() == 11);
   assert(strcmp(string2->c_str(), "hello there") == 0);
   
   delete[] string_serial;
   delete string1;
   delete string2;
   printf("String serialization passed!\n");
}

void test_ack() {
    String* ip1 = new String("172.10.64.31");
    String* ip2 = new String("10.221.22.2");
    Ack* ack_message = new Ack(ip1, ip2);
    assert(ack_message->get_kind() == MsgKind::Ack);

    char* serial = ack_message->serialize();
    Ack* ack_deserial = Ack::deserialize(serial);
    assert(ack_deserial->get_kind() == MsgKind::Ack);
    assert(ack_deserial->get_sender()->equals(ip1));
    assert(ack_deserial->get_target()->equals(ip2));

    delete ip1;
    delete ip2;
    delete ack_message;
    delete serial;
    delete ack_deserial;
    printf("Ack serialization passed!\n");
}

void test_kill() {
    String ip1("172.10.64.31");
    String ip2("10.221.22.2");
    Kill kill_message(&ip2, &ip1);
    assert(kill_message.get_sender()->equals(&ip2));
    assert(kill_message.get_target()->equals(&ip1));
    assert(kill_message.get_kind() == MsgKind::Kill);

    char* kill_serial = kill_message.serialize();
    Message* message = deserialize_message(kill_serial);
    Kill* kill_deserial = reinterpret_cast<Kill*>(message);

    assert(kill_deserial->get_sender()->equals(&ip2));
    assert(kill_deserial->get_target()->equals(&ip1));
    assert(kill_deserial->get_kind() == MsgKind::Kill);

    delete kill_serial;
    delete kill_deserial;
    printf("Kill serialization passed!\n");   
}

void test_register() {
    String ip1("172.101.64.312");
    String ip2("10.221.22.31");
    Register register_message(&ip2, &ip1);
    assert(register_message.get_sender()->equals(&ip2));
    assert(register_message.get_target()->equals(&ip1));
    assert(register_message.get_kind() == MsgKind::Register);

    char* register_serial = register_message.serialize();
    Message* message = deserialize_message(register_serial);
    Register* register_deserial = reinterpret_cast<Register*>(message);

    assert(register_deserial->get_sender()->equals(&ip2));
    assert(register_deserial->get_target()->equals(&ip1));
    assert(register_deserial->get_kind() == MsgKind::Register);

    delete register_serial;
    delete register_deserial;
    printf("Register serialization passed!\n");   
}

void test_put() {
    String ip1("172.10.64.31");
    String ip2("10.221.22.2");
    String msg("Hello from your message.\n");
    Put put_message(&ip1, &ip2, &msg);
    assert(put_message.get_sender()->equals(&ip1));
    assert(put_message.get_target()->equals(&ip2));
    assert(put_message.get_message()->equals(&msg));
    assert(put_message.get_kind() == MsgKind::Put);
    char* put_serial = put_message.serialize();

    Message* message = deserialize_message(put_serial);
    Put* put_deserial = reinterpret_cast<Put*>(message);
    assert(put_deserial->get_sender()->equals(&ip1));
    assert(put_deserial->get_target()->equals(&ip2));
    assert(put_deserial->get_message()->equals(&msg));

    delete put_serial;
    delete put_deserial;
    printf("Put serialization passed!\n");
}

void test_directory() {
    String ip1("172.10.64.31");
    String ip2("10.221.22.2");
    size_t addresses_len = 2;
    String* addresses[addresses_len];
    String ip3("12.132.92.12");
    String ip4("192.168.0.2");
    addresses[0] = &ip3;
    addresses[1] = &ip4;

    Directory directory_message(&ip1, &ip2, addresses, addresses_len);
    assert(directory_message.get_sender()->equals(&ip1));
    assert(directory_message.get_target()->equals(&ip2));
    assert(directory_message.get_addresses()[0]->equals(&ip3));
    assert(directory_message.get_addresses()[1]->equals(&ip4));
    assert(directory_message.get_num() == addresses_len);
    assert(directory_message.get_kind() == MsgKind::Directory);

    char* directory_serial = directory_message.serialize();
    Message* message = deserialize_message(directory_serial);
    Directory* directory_deserial = reinterpret_cast<Directory*>(message);
    assert(directory_deserial->get_sender()->equals(&ip1));
    assert(directory_deserial->get_target()->equals(&ip2));
    assert(directory_deserial->get_addresses()[0]->equals(&ip3));
    assert(directory_deserial->get_addresses()[1]->equals(&ip4));
    assert(directory_deserial->get_num() == addresses_len);

    addresses_len += 2;
    String* addresses2[addresses_len];
    addresses2[0] = &ip1;
    addresses2[1] = &ip2;
    addresses2[2] = &ip3;
    addresses2[3] = &ip4;
    
    Directory directory_message2(&ip4, &ip3, addresses2, addresses_len);
    assert(directory_message2.get_sender()->equals(&ip4));
    assert(directory_message2.get_target()->equals(&ip3));
    assert(directory_message2.get_addresses()[0]->equals(&ip1));
    assert(directory_message2.get_addresses()[1]->equals(&ip2));
    assert(directory_message2.get_addresses()[2]->equals(&ip3));
    assert(directory_message2.get_addresses()[3]->equals(&ip4));
    assert(directory_message2.get_num() == addresses_len);
    assert(directory_message2.get_kind() == MsgKind::Directory);

    char* directory_serial2 = directory_message2.serialize();
    Message* message2 = deserialize_message(directory_serial2);
    Directory* directory_deserial2 = reinterpret_cast<Directory*>(message2);
    assert(directory_deserial2->get_sender()->equals(&ip4));
    assert(directory_deserial2->get_target()->equals(&ip3));
    assert(directory_deserial2->get_addresses()[0]->equals(&ip1));
    assert(directory_deserial2->get_addresses()[1]->equals(&ip2));
    assert(directory_deserial2->get_addresses()[2]->equals(&ip3));
    assert(directory_deserial2->get_addresses()[3]->equals(&ip4));
    assert(directory_deserial2->get_num() == addresses_len);

    delete directory_serial;
    delete directory_deserial;
    delete directory_serial2;
    delete directory_deserial2;
    printf("Directory serialization passed!\n");
}

void test_bool_array() {
    bool bool1 = true;
    bool bool2 = -1;
    bool bool3 = false;
    size_t array_size = 44;
    BoolArray bool_array(array_size);
    bool_array.push(bool1);
    bool_array.push(bool2);
    bool_array.push(bool3);
    size_t array_count = 3;

    assert(bool_array.length() == array_count);
    assert(bool_array.get(0) == bool1);
    assert(bool_array.get(1) == bool2);
    assert(bool_array.get(2) == bool3);

    char* array_serial = bool_array.serialize();
    BoolArray* bool_array2 = BoolArray::deserialize(array_serial);

    assert(bool_array2->length() == array_count);
    assert(bool_array2->get(0) == bool1);
    assert(bool_array2->get(1) == bool2);
    assert(bool_array2->get(2) == bool3);

    bool_array2->push(bool2);
    bool_array2->push(bool3);
    bool_array2->push(bool1);

    char* array_serial2 = bool_array2->serialize();
    BoolArray* bool_array3 = BoolArray::deserialize(array_serial2);

    assert(bool_array3->length() == array_count + 3);
    assert(bool_array3->get(0) == bool1);
    assert(bool_array3->get(1) == bool2);
    assert(bool_array3->get(2) == bool3);
    assert(bool_array3->get(3) == bool2);
    assert(bool_array3->get(4) == bool3);
    assert(bool_array3->get(5) == bool1);

    delete array_serial;
    delete array_serial2;
    delete bool_array2;
    delete bool_array3;
    printf("BoolArray serialization passed!\n");
}

void test_float_array() {
    float float1 = 12.34;
    float float2 = 23.2233;
    float float3 = -12.1111;
    size_t array_size = 44;
    FloatArray float_array(array_size);
    float_array.push(float1);
    float_array.push(float2);
    float_array.push(float3);
    size_t array_count = 3;

    assert(float_array.length() == array_count);
    assert(float_array.get(0) == float1);
    assert(float_array.get(1) == float2);
    assert(float_array.get(2) == float3);

    char* array_serial = float_array.serialize();
    FloatArray* float_array2 = FloatArray::deserialize(array_serial);

    assert(float_array2->length() == array_count);
    assert(float_array2->get(0) == float1);
    assert(float_array2->get(1) == float2);
    assert(float_array2->get(2) == float3);

    float_array2->push(float2);
    float_array2->push(float3);
    float_array2->push(float1);

    char* array_serial2 = float_array2->serialize();
    FloatArray* float_array3 = FloatArray::deserialize(array_serial2);

    assert(float_array3->length() == array_count + 3);
    assert(float_array3->get(0) == float1);
    assert(float_array3->get(1) == float2);
    assert(float_array3->get(2) == float3);
    assert(float_array3->get(3) == float2);
    assert(float_array3->get(4) == float3);
    assert(float_array3->get(5) == float1);

    delete array_serial;
    delete array_serial2;
    delete float_array2;
    delete float_array3;
    printf("FloatArray serialization passed!\n");
}

void test_int_array() {
    int int1 = 1;
    int int2 = -23;
    int int3 = 122;
    size_t array_size = 44;
    IntArray int_array(array_size);
    int_array.push(int1);
    int_array.push(int2);
    int_array.push(int3);
    size_t array_count = 3;

    assert(int_array.length() == array_count);
    assert(int_array.get(0) == int1);
    assert(int_array.get(1) == int2);
    assert(int_array.get(2) == int3);

    char* array_serial = int_array.serialize();
    IntArray* int_array2 = IntArray::deserialize(array_serial);

    assert(int_array2->length() == array_count);
    assert(int_array2->get(0) == int1);
    assert(int_array2->get(1) == int2);
    assert(int_array2->get(2) == int3);

    int_array2->push(int2);
    int_array2->push(int3);
    int_array2->push(int1);

    char* array_serial2 = int_array2->serialize();
    IntArray* int_array3 = IntArray::deserialize(array_serial2);

    assert(int_array3->length() == array_count + 3);
    assert(int_array3->get(0) == int1);
    assert(int_array3->get(1) == int2);
    assert(int_array3->get(2) == int3);
    assert(int_array3->get(3) == int2);
    assert(int_array3->get(4) == int3);
    assert(int_array3->get(5) == int1);

    delete array_serial;
    delete array_serial2;
    delete int_array2;
    delete int_array3;
    printf("IntArray serialization passed!\n");
}

void test_string_array() {
    String string1("big lol");
    String string2("hellko");
    String string3("check it out");
    String string4("A proper sentence.\n");
    size_t size_of_array = 7;
    StringArray string_array(size_of_array);
    string_array.push(&string1);
    string_array.push(&string2);
    string_array.push(&string3);
    string_array.push(&string4);
    size_t array_count = 4;

    assert(string_array.length() == array_count);
    assert(string_array.get(0)->equals(&string1));
    assert(string_array.get(3)->equals(&string4));

    char* array_serial = string_array.serialize();
    StringArray* string_array2 = StringArray::deserialize(array_serial);

    assert(string_array2->length() == array_count);
    assert(string_array2->get(0)->equals(&string1));
    assert(string_array2->get(3)->equals(&string4));

    string_array2->push(&string1);
    string_array2->push(&string2);
    char* array_serial2 = string_array2->serialize();
    StringArray* string_array3 = StringArray::deserialize(array_serial2);

    assert(string_array3->length() == array_count + 2);
    assert(string_array3->get(0)->equals(&string1));
    assert(string_array3->get(3)->equals(&string4));
    assert(string_array3->get(4)->equals(&string1));
    assert(string_array3->get(5)->equals(&string2));

    delete array_serial;
    delete array_serial2;
    delete string_array2;
    delete string_array3;
    printf("StringArray serialization passed!\n");
}

void serializing_test() {
    size_t size_t_value = 55;
    String* string_value = new String("hhihihi");

    size_t serial_len = sizeof(size_t) + sizeof(size_t) + string_value->serial_len();
    Serializer serializer(serial_len);
    serializer.serialize_size_t(serial_len);
    serializer.serialize_size_t(size_t_value);
    serializer.serialize_object(string_value);
    char* serial = serializer.get_serial();

    Deserializer deserial(serial);
    size_t string_serial_size = deserial.deserialize_size_t();
    assert(string_serial_size == serial_len);
    size_t size_t_value_2 = deserial.deserialize_size_t();
    assert(size_t_value_2 == size_t_value);
    String* string_value_2 = String::deserialize(deserial);
    assert(string_value->size() == string_value_2->size());
    assert(strcmp(string_value->c_str(), string_value_2->c_str()) == 0);

    delete[] serial;
    delete string_value;
    delete string_value_2;
    printf("General serializing passed!\n");
}

int main(int argc, char const *argv[]) 
{   
    serializing_test();
    test_string();
    test_ack();
    test_put();
    test_directory();
    test_kill();
    test_register();
    test_bool_array();
    test_float_array();
    test_int_array();
    test_string_array();
    printf("All tests passed!\n");
    return 0;
} 