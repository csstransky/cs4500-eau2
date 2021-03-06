// Made by Kaylin Devchand and Cristian Stransky

#define TEST

#include <stdio.h> 
#include <sys/socket.h> 
#include <arpa/inet.h> 
#include <unistd.h> 
#include <string.h> 
#include "../src/dataframe/dataframe.h"
#include "../src/networks/message.h"
#include "../src/helpers/array.h"
#include "../src/kv_store/key.h"

void test_string() {
   String* string1 = new String("hello there");
   assert(string1->size() == 11);
   assert(strcmp(string1->c_str(), "hello there") == 0);

   char* string_serial = string1->serialize();
   Deserializer deserializer(string_serial);
   String* string2 = new String(deserializer);

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
    String message("Ack serialization passed!\n");
    Ack* ack_message = new Ack(ip1, ip2, &message);
    assert(ack_message->get_kind() == MsgKind::Ack);
    assert(ack_message->get_message()->equals(&message));

    char* serial = ack_message->serialize();
    Ack* ack_deserial = dynamic_cast<Ack*>(Message::deserialize_message(serial));
    assert(ack_deserial->get_kind() == MsgKind::Ack);
    assert(ack_deserial->get_sender()->equals(ip1));
    assert(ack_deserial->get_target()->equals(ip2));
    assert(ack_deserial->get_message()->equals(&message));

    delete ip1;
    delete ip2;
    delete ack_message;
    delete[] serial;
    printf("%s", ack_deserial->get_message()->c_str());
    delete ack_deserial;
}

void test_kill() {
    String ip1("172.10.64.31");
    String ip2("10.221.22.2");
    Kill kill_message(&ip2, &ip1);
    assert(kill_message.get_sender()->equals(&ip2));
    assert(kill_message.get_target()->equals(&ip1));
    assert(kill_message.get_kind() == MsgKind::Kill);

    char* kill_serial = kill_message.serialize();
    Message* message = Message::deserialize_message(kill_serial);
    Kill* kill_deserial = reinterpret_cast<Kill*>(message);

    assert(kill_deserial->get_sender()->equals(&ip2));
    assert(kill_deserial->get_target()->equals(&ip1));
    assert(kill_deserial->get_kind() == MsgKind::Kill);

    delete[] kill_serial;
    delete kill_deserial;
    printf("Kill serialization passed!\n");   
}

void test_register() {
    String ip1("172.101.64.312");
    String ip2("10.221.22.31");
    size_t node_index = 12;
    Register register_message(&ip2, &ip1, node_index);
    assert(register_message.get_sender()->equals(&ip2));
    assert(register_message.get_target()->equals(&ip1));
    assert(register_message.get_node_index() == node_index);
    assert(register_message.get_kind() == MsgKind::Register);

    char* register_serial = register_message.serialize();
    Message* message = Message::deserialize_message(register_serial);
    Register* register_deserial = reinterpret_cast<Register*>(message);

    assert(register_deserial->get_sender()->equals(&ip2));
    assert(register_deserial->get_target()->equals(&ip1));
    assert(register_deserial->get_kind() == MsgKind::Register);
    assert(register_deserial->get_node_index() == node_index);

    delete[] register_serial;
    delete register_deserial;
    printf("Register serialization passed!\n");   
}

void test_put() {
    String ip1("172.10.64.31");
    String ip2("10.221.22.2");
    String key("keykey");
    size_t int_array_size = 100;
    IntArray int_array(int_array_size);
    for (size_t ii = 0; ii < int_array_size; ii++) {
        int_array.push(ii);
    }
    char* int_array_serial = int_array.serialize();
    Serializer serializer(int_array_serial, int_array.serial_len());

    Put put_message(&ip1, &ip2, &key, &serializer);
    assert(put_message.get_sender()->equals(&ip1));
    assert(put_message.get_target()->equals(&ip2));
    assert(put_message.get_key_name()->equals(&key));
    assert(put_message.get_value()->equals(&serializer));
    assert(put_message.get_kind() == MsgKind::Put);

    char* put_serial = put_message.serialize();
    Message* message = Message::deserialize_message(put_serial);
    Put* put_deserial = reinterpret_cast<Put*>(message);
    assert(put_deserial->get_sender()->equals(&ip1));
    assert(put_deserial->get_target()->equals(&ip2));
    assert(put_deserial->get_key_name()->equals(&key));
    assert(put_deserial->get_value()->equals(&serializer));
    assert(put_deserial->get_kind() == MsgKind::Put);

    char* int_array_serial2 = put_deserial->get_serial();
    Deserializer deserializer(int_array_serial2);
    IntArray* deserial_int_array = new IntArray(deserializer);
    assert(deserial_int_array->length() == int_array_size);
    for (size_t ii = 0; ii < deserial_int_array->length(); ii++) {
        assert(deserial_int_array->get(ii) == ii);
    }

    delete[] int_array_serial;
    delete[] int_array_serial2;
    delete deserial_int_array;
    delete[] put_serial;
    delete put_deserial;
    printf("Put serialization passed!\n");
}

void test_get() {
    String ip1("172.10.64.31");
    String ip2("10.221.22.2");
    String key("keykey");
    Get get_message(&ip1, &ip2, &key);
    assert(get_message.get_sender()->equals(&ip1));
    assert(get_message.get_target()->equals(&ip2));
    assert(get_message.get_key_name()->equals(&key));
    assert(get_message.get_kind() == MsgKind::Get);

    char* get_serial = get_message.serialize();
    Message* message = Message::deserialize_message(get_serial);
    Get* get_deserial = reinterpret_cast<Get*>(message);
    assert(get_deserial->get_sender()->equals(&ip1));
    assert(get_deserial->get_target()->equals(&ip2));
    assert(get_deserial->get_key_name()->equals(&key));
    assert(get_deserial->get_kind() == MsgKind::Get);

    delete[] get_serial;
    delete get_deserial;
    printf("Get serialization passed!\n");
}

void test_wait_get() {
    String ip1("172.10.64.31");
    String ip2("10.221.22.2");
    String key("keykey");
    WaitAndGet get_message(&ip1, &ip2, &key);
    assert(get_message.get_sender()->equals(&ip1));
    assert(get_message.get_target()->equals(&ip2));
    assert(get_message.get_key_name()->equals(&key));
    assert(get_message.get_kind() == MsgKind::WaitAndGet);

    char* get_serial = get_message.serialize();
    Message* message = Message::deserialize_message(get_serial);
    WaitAndGet* get_deserial = reinterpret_cast<WaitAndGet*>(message);
    assert(get_deserial->get_sender()->equals(&ip1));
    assert(get_deserial->get_target()->equals(&ip2));
    assert(get_deserial->get_key_name()->equals(&key));
    assert(get_deserial->get_kind() == MsgKind::WaitAndGet);

    delete[] get_serial;
    delete get_deserial;
    printf("WaitAndGet serialization passed!\n");
}

void test_value() {
    String ip1("172.10.64.31");
    String ip2("10.221.22.2");
    size_t string_array_size = 103;
    StringArray string_array(string_array_size);
    for (size_t ii = 0; ii < string_array_size; ii++) {
        String base_string("base_");
        base_string.concat(ii);
        string_array.push(&base_string);
    }
    char* string_array_serial = string_array.serialize();
    Serializer serializer(string_array_serial, string_array.serial_len());

    Value value_message(&ip1, &ip2, &serializer);
    assert(value_message.get_sender()->equals(&ip1));
    assert(value_message.get_target()->equals(&ip2));
    assert(value_message.get_value()->equals(&serializer));
    assert(value_message.get_kind() == MsgKind::Value);

    char* value_serial = value_message.serialize();
    Message* message = Message::deserialize_message(value_serial);
    Value* value_deserial = reinterpret_cast<Value*>(message);
    assert(value_deserial->get_sender()->equals(&ip1));
    assert(value_deserial->get_target()->equals(&ip2));
    assert(value_deserial->get_value()->equals(&serializer));
    assert(value_deserial->get_kind() == MsgKind::Value);

    char* string_array_serial2 = value_deserial->get_serial();
    Deserializer deserializer(string_array_serial2);
    StringArray* deserial_string_array = new StringArray(deserializer);
    assert(deserial_string_array->length() == string_array_size);
    for (size_t ii = 0; ii < deserial_string_array->length(); ii++) {
        String base_string("base_");
        base_string.concat(ii);
        assert(base_string.equals(deserial_string_array->get(ii)));
    }

    delete[] string_array_serial;
    delete[] string_array_serial2;
    delete deserial_string_array;
    delete[] value_serial;
    delete value_deserial;
    printf("Value serialization passed!\n");
}

void test_directory() {
    String ip1("172.10.64.31");
    String ip2("10.221.22.2");
    size_t addresses_len = 2;
    StringArray addresses(addresses_len);
    String ip3("12.132.92.12");
    String ip4("192.168.0.2");
    addresses.push(&ip3);
    addresses.push(&ip4);
    IntArray nodes(addresses_len);
    size_t node_1 = 2;
    size_t node_2 = 3;
    size_t node_3 = 12;
    size_t node_4 = 13;
    nodes.push(node_1);
    nodes.push(node_2);

    Directory directory_message(&ip1, &ip2, &addresses, &nodes);
    assert(directory_message.get_sender()->equals(&ip1));
    assert(directory_message.get_target()->equals(&ip2));
    assert(directory_message.get_addresses()->length() == addresses_len);
    assert(directory_message.get_addresses()->get(0)->equals(&ip3));
    assert(directory_message.get_addresses()->get(1)->equals(&ip4));
    assert(directory_message.get_node_indexes()->length() == addresses_len);
    assert(directory_message.get_node_indexes()->get(0) == node_1);
    assert(directory_message.get_node_indexes()->get(1) == node_2);
    assert(directory_message.get_kind() == MsgKind::Directory);

    char* directory_serial = directory_message.serialize();
    Message* message = Message::deserialize_message(directory_serial);
    Directory* directory_deserial = reinterpret_cast<Directory*>(message);
    assert(directory_deserial->get_sender()->equals(&ip1));
    assert(directory_deserial->get_target()->equals(&ip2));
    assert(directory_deserial->get_addresses()->get(0)->equals(&ip3));
    assert(directory_deserial->get_addresses()->get(1)->equals(&ip4));
    assert(directory_deserial->get_node_indexes()->get(0) == node_1);
    assert(directory_deserial->get_node_indexes()->get(1) == node_2);
    assert(directory_deserial->get_kind() == MsgKind::Directory);

    addresses_len += 2;
    StringArray addresses2(addresses_len);
    addresses2.push(&ip1);
    addresses2.push(&ip2);
    addresses2.push(&ip3);
    addresses2.push(&ip4);
    IntArray nodes2(addresses_len);
    nodes2.push(node_1);
    nodes2.push(node_2);
    nodes2.push(node_3);
    nodes2.push(node_4);
    
    Directory directory_message2(&ip4, &ip3, &addresses2, &nodes2);
    assert(directory_message2.get_sender()->equals(&ip4));
    assert(directory_message2.get_target()->equals(&ip3));
    assert(directory_message2.get_addresses()->get(0)->equals(&ip1));
    assert(directory_message2.get_addresses()->get(1)->equals(&ip2));
    assert(directory_message2.get_addresses()->get(2)->equals(&ip3));
    assert(directory_message2.get_addresses()->get(3)->equals(&ip4));
    assert(directory_message2.get_kind() == MsgKind::Directory);
    assert(directory_message2.get_node_indexes()->get(0) == node_1);
    assert(directory_message2.get_node_indexes()->get(1) == node_2);
    assert(directory_message2.get_node_indexes()->get(2) == node_3);
    assert(directory_message2.get_node_indexes()->get(3) == node_4);

    char* directory_serial2 = directory_message2.serialize();
    Message* message2 = Message::deserialize_message(directory_serial2);
    Directory* directory_deserial2 = reinterpret_cast<Directory*>(message2);
    assert(directory_deserial2->get_sender()->equals(&ip4));
    assert(directory_deserial2->get_target()->equals(&ip3));
    assert(directory_deserial2->get_addresses()->get(0)->equals(&ip1));
    assert(directory_deserial2->get_addresses()->get(1)->equals(&ip2));
    assert(directory_deserial2->get_addresses()->get(2)->equals(&ip3));
    assert(directory_deserial2->get_addresses()->get(3)->equals(&ip4));
    assert(directory_deserial2->get_kind() == MsgKind::Directory);
    assert(directory_deserial2->get_node_indexes()->get(0) == node_1);
    assert(directory_deserial2->get_node_indexes()->get(1) == node_2);
    assert(directory_deserial2->get_node_indexes()->get(2) == node_3);
    assert(directory_deserial2->get_node_indexes()->get(3) == node_4);

    delete[] directory_serial;
    delete directory_deserial;
    delete[] directory_serial2;
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
    Deserializer deserializer(array_serial);
    BoolArray* bool_array2 = new BoolArray(deserializer);

    assert(bool_array2->length() == array_count);
    assert(bool_array2->get(0) == bool1);
    assert(bool_array2->get(1) == bool2);
    assert(bool_array2->get(2) == bool3);

    bool_array2->push(bool2);
    bool_array2->push(bool3);
    bool_array2->push(bool1);

    char* array_serial2 = bool_array2->serialize();
    Deserializer deserializer1(array_serial2);
    BoolArray* bool_array3 = new BoolArray(deserializer1);

    assert(bool_array3->length() == array_count + 3);
    assert(bool_array3->get(0) == bool1);
    assert(bool_array3->get(1) == bool2);
    assert(bool_array3->get(2) == bool3);
    assert(bool_array3->get(3) == bool2);
    assert(bool_array3->get(4) == bool3);
    assert(bool_array3->get(5) == bool1);

    delete[] array_serial;
    delete[] array_serial2;
    delete bool_array2;
    delete bool_array3;
    printf("BoolArray serialization passed!\n");
}

void test_double_array() {
    double double1 = 12.34;
    double double2 = 23.2233;
    double double3 = -12.1111;
    size_t array_size = 44;
    DoubleArray double_array(array_size);
    double_array.push(double1);
    double_array.push(double2);
    double_array.push(double3);
    size_t array_count = 3;

    assert(double_array.length() == array_count);
    assert(double_array.get(0) == double1);
    assert(double_array.get(1) == double2);
    assert(double_array.get(2) == double3);

    char* array_serial = double_array.serialize();
    Deserializer deserializer(array_serial);
    DoubleArray* double_array2 = new DoubleArray(deserializer);

    assert(double_array2->length() == array_count);
    assert(double_array2->get(0) == double1);
    assert(double_array2->get(1) == double2);
    assert(double_array2->get(2) == double3);

    double_array2->push(double2);
    double_array2->push(double3);
    double_array2->push(double1);

    char* array_serial2 = double_array2->serialize();
    Deserializer deserializer1(array_serial2);
    DoubleArray* double_array3 = new DoubleArray(deserializer1);

    assert(double_array3->length() == array_count + 3);
    assert(double_array3->get(0) == double1);
    assert(double_array3->get(1) == double2);
    assert(double_array3->get(2) == double3);
    assert(double_array3->get(3) == double2);
    assert(double_array3->get(4) == double3);
    assert(double_array3->get(5) == double1);

    delete[] array_serial;
    delete[] array_serial2;
    delete double_array2;
    delete double_array3;
    printf("DoubleArray serialization passed!\n");
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
    Deserializer deserializer(array_serial);
    IntArray* int_array2 = new IntArray(deserializer);

    assert(int_array2->length() == array_count);
    assert(int_array2->get(0) == int1);
    assert(int_array2->get(1) == int2);
    assert(int_array2->get(2) == int3);

    int_array2->push(int2);
    int_array2->push(int3);
    int_array2->push(int1);

    char* array_serial2 = int_array2->serialize();
    Deserializer deserializer1(array_serial2);
    IntArray* int_array3 = new IntArray(deserializer1);

    assert(int_array3->length() == array_count + 3);
    assert(int_array3->get(0) == int1);
    assert(int_array3->get(1) == int2);
    assert(int_array3->get(2) == int3);
    assert(int_array3->get(3) == int2);
    assert(int_array3->get(4) == int3);
    assert(int_array3->get(5) == int1);

    delete[] array_serial;
    delete[] array_serial2;
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
    Deserializer deserializer1(array_serial);
    StringArray* string_array2 = new StringArray(deserializer1);

    assert(string_array2->length() == array_count);
    assert(string_array2->get(0)->equals(&string1));
    assert(string_array2->get(3)->equals(&string4));

    string_array2->push(&string1);
    string_array2->push(&string2);
    char* array_serial2 = string_array2->serialize();
    Deserializer deserializer(array_serial2);
    StringArray* string_array3 = new StringArray(deserializer);

    assert(string_array3->length() == array_count + 2);
    assert(string_array3->get(0)->equals(&string1));
    assert(string_array3->get(3)->equals(&string4));
    assert(string_array3->get(4)->equals(&string1));
    assert(string_array3->get(5)->equals(&string2));

    delete[] array_serial;
    delete[] array_serial2;
    delete string_array2;
    delete string_array3;
    printf("StringArray serialization passed!\n");
}

void test_key() {
    String key_string("main");
    size_t node_index = 92;
    Key key(&key_string, node_index);
    char* serial = key.serialize();
    Deserializer deserializer(serial);
    Key* deserial_key = new Key(deserializer);
    assert(key_string.equals(deserial_key->get_key()));
    assert(node_index == deserial_key->get_node_index());
    assert(key.equals(deserial_key));
    delete[] serial;
    delete deserial_key;
    printf("Key serialization passed!\n");
}

void test_schema() {
    char* schema_type = const_cast<char*>("IDSBDISIBDSIBDDSSIBD");
    size_t num_cols = 20;
    size_t num_rows = 34;
    Schema schema(schema_type);
    for (size_t ii = 0; ii < num_rows; ii++) {
        schema.add_row();
    }
    assert(schema.num_cols_ == num_cols);
    assert(schema.num_rows_ == num_rows);
    assert(strcmp(schema.types_->c_str(), schema_type) == 0);

    char* schema_serial = schema.serialize();
    Deserializer deserializer(schema_serial);
    Schema* deserial_schema = new Schema(deserializer);
    assert(deserial_schema->num_cols_ == num_cols);
    assert(deserial_schema->num_rows_ == num_rows);
    assert(strcmp(deserial_schema->types_->c_str(), schema_type) == 0);
    assert(deserial_schema->types_->size() == schema.types_->size());
    assert(schema.equals(deserial_schema));

    delete[] schema_serial;
    delete deserial_schema;
    printf("Schema serialization passed!\n");
}

void test_int_column() {
    String df_name("mainframe");
    size_t local_node_index = 4;
    Key df_key(&df_name, local_node_index);
    KV_Store kv(local_node_index);
    Column int_column('I', &kv);

    size_t buffered_elements_size = 10;
    size_t number_of_kv_chunks = 4;
    size_t int_column_count = ELEMENT_ARRAY_SIZE * number_of_kv_chunks + buffered_elements_size;
    IntArray int_array(ELEMENT_ARRAY_SIZE);
    for (size_t ii = 0; ii < int_column_count; ii++) {
        if (ii % ELEMENT_ARRAY_SIZE == 0 && ii > 0) {
            String stored_string(df_name);
            stored_string.concat('_');
            stored_string.concat(local_node_index);
            stored_string.concat('_');
            stored_string.concat(ii / ELEMENT_ARRAY_SIZE - 1);
            Key int_chunk_key(&stored_string, local_node_index);
            int_column.push_back(&int_array, &int_chunk_key);
            int_array.clear();
        }
        int_array.push(ii);
    }
    if (int_array.length() > 0) {
        String stored_string(df_name);
        stored_string.concat('_');
        stored_string.concat(local_node_index);
        stored_string.concat('_');
        stored_string.concat(number_of_kv_chunks + 1);
        Key int_chunk_key(&stored_string, local_node_index);
        int_column.push_back(&int_array, &int_chunk_key);
    }
    assert(int_column.keys_->length() == number_of_kv_chunks + 1);
    assert(int_column.size() == int_column_count);

    // Test that the Column serializes and deserializes correctly
    char* int_col_serial = int_column.serialize();
    Deserializer deserializer(int_col_serial);
    Column* deserial_int_col = new Column(deserializer, &kv);
    for (size_t ii = 0; ii < deserial_int_col->size(); ii++) {
        assert(deserial_int_col->get_int(ii) == ii);
    }
    assert(deserial_int_col->keys_->length() == number_of_kv_chunks + 1);
    assert(deserial_int_col->size_ == int_column_count);
    assert(deserial_int_col->type_ == 'I');
    
    // Test that each kv chunk is still correct
    for (size_t ii = 0; ii < number_of_kv_chunks; ii++) {
        String stored_string(df_name);
        stored_string.concat('_');
        stored_string.concat(local_node_index);
        stored_string.concat('_');
        stored_string.concat(ii);
        Key stored_element_key(&stored_string, local_node_index);
        Array* stored_ints = deserial_int_col->kv_->get_array(&stored_element_key, 'I');
        size_t starting_index = ELEMENT_ARRAY_SIZE * ii;
        for (size_t jj = 0; jj < ELEMENT_ARRAY_SIZE; jj++) {
            assert(stored_ints->get(jj).i == starting_index + jj);
            assert(stored_ints->get(jj).i == deserial_int_col->get_int(starting_index + jj));
        }
        delete stored_ints;
    }
    
    // Test that the stored elements are all still the same
    for (size_t ii = 0; ii < deserial_int_col->size(); ii++) {
        assert(deserial_int_col->get_int(ii) == ii);
    }

    delete[] int_col_serial;
    delete deserial_int_col;
    printf("IntColumn serialization passed!\n");
}

void test_double_column() {
    String df_name("doubleies");
    size_t local_node_index = 4;
    Key df_key(&df_name, local_node_index);
    KV_Store kv(local_node_index);
    Column double_column('D', &kv);

    size_t buffered_elements_size = 18;
    size_t number_of_kv_chunks = 3;
    double double_decimal = 0.002;
    size_t double_column_count = ELEMENT_ARRAY_SIZE * number_of_kv_chunks + buffered_elements_size;
    DoubleArray chunk_array(ELEMENT_ARRAY_SIZE);
    for (size_t ii = 0; ii < double_column_count; ii++) {
        if (ii % ELEMENT_ARRAY_SIZE == 0 && ii > 0) {
            String stored_string(df_name);
            stored_string.concat('_');
            stored_string.concat(local_node_index);
            stored_string.concat('_');
            stored_string.concat(ii / ELEMENT_ARRAY_SIZE - 1);
            Key stored_element_key(&stored_string, local_node_index);
            double_column.push_back(&chunk_array, &stored_element_key);
            chunk_array.clear();
        }
        chunk_array.push(ii + double_decimal);
    }
    if (chunk_array.length() > 0) {
        String stored_string(df_name);
        stored_string.concat('_');
        stored_string.concat(local_node_index);
        stored_string.concat('_');
        stored_string.concat(number_of_kv_chunks + 1);
        Key stored_element_key(&stored_string, local_node_index);
        double_column.push_back(&chunk_array, &stored_element_key);
    }
    assert(chunk_array.size_ == ELEMENT_ARRAY_SIZE);
    assert(double_column.size() == double_column_count);
    assert(double_column.keys_->length() == number_of_kv_chunks + 1);

    // Test that the Column serializes and deserializes correctly
    char* double_col_serial = double_column.serialize();
    Deserializer deserializer(double_col_serial);
    Column* deserial_double_col = new Column(deserializer, &kv);
    for (size_t ii = 0; ii < deserial_double_col->size(); ii++) {
        assert(deserial_double_col->get_double(ii) == ii + double_decimal);
    }
    assert(deserial_double_col->keys_->length() == number_of_kv_chunks + 1);
    assert(deserial_double_col->size_ == double_column_count);
    assert(deserial_double_col->type_ == 'D');
    
    // Test that each kv chunk is still correct
    for (size_t ii = 0; ii < number_of_kv_chunks; ii++) {
        String stored_string(df_name);
        stored_string.concat('_');
        stored_string.concat(local_node_index);
        stored_string.concat('_');
        stored_string.concat(ii);
        Key stored_element_key(&stored_string, local_node_index);
        Array* stored_doubles = deserial_double_col->kv_->get_array(&stored_element_key, 'D');
        size_t starting_index = ELEMENT_ARRAY_SIZE * ii;
        for (size_t jj = 0; jj < ELEMENT_ARRAY_SIZE; jj++) {
            assert(stored_doubles->get(jj).d == starting_index + jj + double_decimal);
            assert(stored_doubles->get(jj).d == deserial_double_col->get_double(starting_index + jj));
        }
        delete stored_doubles;
    }

    // Test that the buffered elements are still the same
    for (size_t ii = 0; ii < deserial_double_col->size(); ii++) {
        assert(deserial_double_col->get_double(ii) == ii + double_decimal);
    }

    delete[] double_col_serial;
    delete deserial_double_col;
    printf("DoubleColumn serialization passed!\n");
}

void test_bool_column() {
    String df_name("switches");
    size_t local_node_index = 6;
    Key df_key(&df_name, local_node_index);
    KV_Store kv(local_node_index);
    Column bool_column('B', &kv);

    size_t buffered_elements_size = 98;
    size_t number_of_kv_chunks = 5;
    size_t bool_column_count = ELEMENT_ARRAY_SIZE * number_of_kv_chunks + buffered_elements_size;
    BoolArray chunk_array(ELEMENT_ARRAY_SIZE);
    for (size_t ii = 0; ii < bool_column_count; ii += 2) {
        if (ii % ELEMENT_ARRAY_SIZE == 0 && ii > 0) {
            String stored_string(df_name);
            stored_string.concat('_');
            stored_string.concat(local_node_index);
            stored_string.concat('_');
            stored_string.concat(ii / ELEMENT_ARRAY_SIZE - 1);
            Key stored_element_key(&stored_string, local_node_index);
            bool_column.push_back(&chunk_array, &stored_element_key);
            chunk_array.clear();
        }
        chunk_array.push(true);
        chunk_array.push(false);
    }
    if (chunk_array.length() > 0) {
        String stored_string(df_name);
        stored_string.concat('_');
        stored_string.concat(local_node_index);
        stored_string.concat('_');
        stored_string.concat(number_of_kv_chunks + 1);
        Key stored_element_key(&stored_string, local_node_index);
        bool_column.push_back(&chunk_array, &stored_element_key);
    }
    assert(chunk_array.size_ == ELEMENT_ARRAY_SIZE); // This test only works if this value is even
    assert(bool_column.size() == bool_column_count);
    assert(bool_column.keys_->length() == number_of_kv_chunks + 1);

    // Test that the Column serializes and deserializes correctly
    char* bool_col_serial = bool_column.serialize();
    Deserializer deserializer(bool_col_serial);
    Column* deserial_bool_col = new Column(deserializer, &kv);
    for (size_t ii = 0; ii < deserial_bool_col->size(); ii += 2) {
        assert(deserial_bool_col->get_bool(ii));
        assert(!deserial_bool_col->get_bool(ii + 1));
    }
    assert(deserial_bool_col->keys_->length() == number_of_kv_chunks + 1);
    assert(deserial_bool_col->size_ == bool_column_count);
    assert(deserial_bool_col->type_ == 'B');
    
    // Test that each kv chunk is still correct
    for (size_t ii = 0; ii < number_of_kv_chunks; ii++) {
        String stored_string(df_name);
        stored_string.concat('_');
        stored_string.concat(local_node_index);
        stored_string.concat('_');
        stored_string.concat(ii);
        Key stored_element_key(&stored_string, local_node_index);
        Array* stored_bools = deserial_bool_col->kv_->get_array(&stored_element_key, 'B');
        size_t starting_index = ELEMENT_ARRAY_SIZE * ii;
        for (size_t jj = 0; jj < ELEMENT_ARRAY_SIZE; jj += 2) {
            assert(stored_bools->get(jj).b);
            assert(stored_bools->get(jj).b == deserial_bool_col->get_bool(starting_index + jj));
            assert(!stored_bools->get(jj + 1).b);
            assert(stored_bools->get(jj + 1).b == deserial_bool_col->get_bool(starting_index + jj + 1));
        }
        delete stored_bools;
    }

    // Test that the buffered elements are still the same
    for (size_t ii = 0; ii < deserial_bool_col->size(); ii += 2) {
        assert(deserial_bool_col->get_bool(ii));
        assert(!deserial_bool_col->get_bool(ii + 1));
    }

    delete[] bool_col_serial;
    delete deserial_bool_col;
    printf("BoolColumn serialization passed!\n");
}

void test_string_column() {
    String df_name("mainframe");
    String base_string("col_string_");
    size_t local_node_index = 4;
    Key df_key(&df_name, local_node_index);
    KV_Store kv(local_node_index);
    Column string_column('S', &kv);

    size_t buffered_elements_size = 10;
    size_t number_of_kv_chunks = 4;
    size_t string_column_count = ELEMENT_ARRAY_SIZE * number_of_kv_chunks + buffered_elements_size;
    StringArray string_cache(ELEMENT_ARRAY_SIZE);
    for (size_t ii = 0; ii < string_column_count; ii++) {
        if (ii % ELEMENT_ARRAY_SIZE == 0 && ii > 0) {
            String stored_string(df_name);
            stored_string.concat('_');
            stored_string.concat(local_node_index);
            stored_string.concat('_');
            stored_string.concat(ii / ELEMENT_ARRAY_SIZE - 1);
            Key stored_element_key(&stored_string, local_node_index);
            string_column.push_back(&string_cache, &stored_element_key);
            string_cache.clear();
        }
        String temp_string(base_string);
        temp_string.concat(ii);
        string_cache.push(&temp_string);
    }
    if (string_cache.length() > 0) {
        String stored_string(df_name);
        stored_string.concat('_');
        stored_string.concat(local_node_index);
        stored_string.concat('_');
        stored_string.concat(number_of_kv_chunks + 1);
        Key stored_element_key(&stored_string, local_node_index);
        string_column.push_back(&string_cache, &stored_element_key);
    }
    assert(string_column.size() == string_column_count);
    assert(string_cache.size_ == ELEMENT_ARRAY_SIZE);
    assert(string_column.keys_->length() == number_of_kv_chunks + 1);

    // Test that the Column serializes and deserializes correctly
    char* string_col_serial = string_column.serialize();
    Deserializer deserializer(string_col_serial);
    Column* deserial_string_col = new Column(deserializer, &kv);
    for (size_t ii = 0; ii < deserial_string_col->size(); ii++) {
        String temp_string(base_string);
        temp_string.concat(ii);
        String* kv_string = deserial_string_col->get_string(ii);
        assert(kv_string->equals(&temp_string));
    }
    assert(deserial_string_col->keys_->length() == number_of_kv_chunks + 1);
    assert(deserial_string_col->size_ == string_column_count);
    assert(deserial_string_col->type_ == 'S');
    
    // Test that each kv chunk is still correct
    for (size_t ii = 0; ii < number_of_kv_chunks; ii++) {
        String stored_string(df_name);
        stored_string.concat('_');
        stored_string.concat(local_node_index);
        stored_string.concat('_');
        stored_string.concat(ii);
        Key stored_element_key(&stored_string, local_node_index);
        Array* stored_strings = deserial_string_col->kv_->get_array(&stored_element_key, 'S');
        size_t starting_index = ELEMENT_ARRAY_SIZE * ii;
        for (size_t jj = 0; jj < ELEMENT_ARRAY_SIZE; jj++) {
            String temp_string(base_string);
            temp_string.concat(starting_index + jj);
            assert(stored_strings->get(jj).o->equals(&temp_string));
            String* kv_string = deserial_string_col->get_string(starting_index + jj);
            assert(stored_strings->get(jj).o->equals(kv_string));
        }
        delete stored_strings;
    }

    // Test that the buffered elements are still the same
    for (size_t ii = 0; ii < deserial_string_col->size(); ii++) {
        String temp_string(base_string);
        temp_string.concat(ii);
        assert(deserial_string_col->get_string(ii)->equals(&temp_string));
    }

    delete[] string_col_serial;
    delete deserial_string_col;
    printf("StringColumn serialization passed!\n");
}

void test_column_array() {
    size_t local_node_index = 3;

    KV_Store kv(local_node_index);
    Column string_column('S', &kv);
    Column double_column('D', &kv);
    Column bool_column('B', &kv);
    Column int_column('I', &kv);

    size_t buffered_elements_size = 10;
    size_t number_of_kv_chunks = 5;
    size_t string_column_count = ELEMENT_ARRAY_SIZE * number_of_kv_chunks + buffered_elements_size;
    String base_string("col_string_");
    double double_decimal = 0.002;
    StringArray string_cache(ELEMENT_ARRAY_SIZE);
    DoubleArray double_cache(ELEMENT_ARRAY_SIZE);
    IntArray int_cache(ELEMENT_ARRAY_SIZE);
    BoolArray bool_cache(ELEMENT_ARRAY_SIZE);
    for (size_t ii = 0; ii < string_column_count; ii++) {
        if (ii % ELEMENT_ARRAY_SIZE == 0 && ii > 0) {
            String string_col_string("string_col_");
            string_col_string.concat(ii);
            String double_col_string("double_col_");
            double_col_string.concat(ii);
            String int_col_string("int_col_");
            int_col_string.concat(ii);
            String bool_col_string("bool_col_");
            bool_col_string.concat(ii);
            Key string_key(&string_col_string, local_node_index);
            Key double_key(&double_col_string, local_node_index);
            Key int_key(&int_col_string, local_node_index);
            Key bool_key(&bool_col_string, local_node_index);
            string_column.push_back(&string_cache, &string_key);
            double_column.push_back(&double_cache, &double_key);
            int_column.push_back(&int_cache, &int_key);
            bool_column.push_back(&bool_cache, &bool_key);
            string_cache.clear();
            double_cache.clear();
            int_cache.clear();
            bool_cache.clear();
        }
        String temp_string(base_string);
        temp_string.concat(ii);
        string_cache.push(&temp_string);
        double_cache.push(ii + double_decimal);
        int_cache.push(ii);
        bool_cache.push(true);
    }
    if (string_cache.length() > 0) {
        String string_col_string("string_col_");
        string_col_string.concat(number_of_kv_chunks + 1);
        String double_col_string("double_col_");
        double_col_string.concat(number_of_kv_chunks + 1);
        String int_col_string("int_col_");
        int_col_string.concat(number_of_kv_chunks + 1);
        String bool_col_string("bool_col_");
        bool_col_string.concat(number_of_kv_chunks + 1);
        Key string_key(&string_col_string, local_node_index);
        Key double_key(&double_col_string, local_node_index);
        Key int_key(&int_col_string, local_node_index);
        Key bool_key(&bool_col_string, local_node_index);
        string_column.push_back(&string_cache, &string_key);
        double_column.push_back(&double_cache, &double_key);
        int_column.push_back(&int_cache, &int_key);
        bool_column.push_back(&bool_cache, &bool_key);
    }
    assert(string_column.size() == string_column_count);
    assert(int_column.size() == string_column_count);
    assert(bool_column.size() == string_column_count);
    assert(double_column.size() == string_column_count);
    assert(string_column.keys_->length() == number_of_kv_chunks + 1);
    assert(double_column.keys_->length()  == number_of_kv_chunks + 1);
    assert(int_column.keys_->length() == number_of_kv_chunks + 1);
    assert(bool_column.keys_->length() == number_of_kv_chunks + 1);

    size_t col_array_size = 10;
    size_t col_count = 4;
    ColumnArray col_array(col_array_size);
    col_array.push(&string_column);
    col_array.push(&double_column);
    col_array.push(&int_column);
    col_array.push(&bool_column);

    assert(col_array.size_ == col_array_size);
    assert(col_array.length() == col_count);
    for (size_t ii = 0; ii < col_count; ii++) {
        char col_type = col_array.get(ii)->get_type();
        switch (col_type) {
            case 'I': {
                for (size_t jj = 0; jj < string_column_count; jj++) {
                    assert(col_array.get(ii)->get_int(jj) == jj);
                }
                break;
            }
            case 'S': {
                for (size_t jj = 0; jj < string_column_count; jj++) {
                    String temp_string(base_string);
                    temp_string.concat(jj);
                    String* column_string = col_array.get(ii)->get_string(jj);
                    assert(column_string->equals(&temp_string));
                }
                break;
            }
            case 'D': {
                for (size_t jj = 0; jj < string_column_count; jj++) {
                    assert(col_array.get(ii)->get_double(jj) == jj + double_decimal);
                }
                break;
            }
            case 'B': {
                for (size_t jj = 0; jj < string_column_count; jj++) {
                    assert(col_array.get(ii)->get_bool(jj));
                }
                break;
            }
        }
    }

    char* serial = col_array.serialize();
    Deserializer deserializer(serial);
    ColumnArray* deserial_col_array = new ColumnArray(deserializer, &kv);

    assert(deserial_col_array->size_ == col_array_size);
    assert(deserial_col_array->length() == col_count);
    for (size_t ii = 0; ii < col_count; ii++) {
        char col_type = deserial_col_array->get(ii)->get_type();
        switch (col_type) {
            case 'I': {
                for (size_t jj = 0; jj < string_column_count; jj++) {
                    assert(deserial_col_array->get(ii)->get_int(jj) == jj);
                    assert(deserial_col_array->get(ii)->get_int(jj) 
                        == col_array.get(ii)->get_int(jj));
                }
                break;
            }
            case 'S': {
                for (size_t jj = 0; jj < string_column_count; jj++) {
                    String temp_string(base_string);
                    temp_string.concat(jj);
                    String* column_string = col_array.get(ii)->get_string(jj);
                    String* deserial_string = deserial_col_array->get(ii)->get_string(jj);
                    assert(deserial_string->equals(&temp_string));
                    assert(deserial_string->equals(column_string));
                }
                break;
            }
            case 'D': {
                for (size_t jj = 0; jj < string_column_count; jj++) {
                    assert(deserial_col_array->get(ii)->get_double(jj) == jj + double_decimal);
                    assert(deserial_col_array->get(ii)->get_double(jj) 
                        == col_array.get(ii)->get_double(jj));
                }
                break;
            }
            case 'B': {
                for (size_t jj = 0; jj < string_column_count; jj++) {
                    assert(deserial_col_array->get(ii)->get_bool(jj));
                    assert(deserial_col_array->get(ii)->get_bool(jj) 
                        == col_array.get(ii)->get_bool(jj));

                }
                break;
            }
        }
    }

    delete[] serial;
    delete deserial_col_array;
    printf("ColumnArray serialization passed!\n");
}

void test_basic_dataframe() {
    KV_Store kv(0);
    Schema s1("");
    DataFrame df(s1, &kv);

    Column c_int('I', &kv);
    IntArray temp_int(4);
    temp_int.push(1);
    temp_int.push(3);
    temp_int.push(4);
    temp_int.push(2);
    String temp_string("main_int");
    Key temp_key1(&temp_string, 0);
    c_int.push_back(&temp_int, &temp_key1);

    Column c_double('D', &kv);
    DoubleArray temp_double(4);
    temp_double.push((double)1.2);
    temp_double.push((double)3.2);
    temp_double.push((double)2);
    temp_double.push((double)1);
    temp_string.concat("_double");
    Key temp_key2(&temp_string, 0);
    c_double.push_back(&temp_double, &temp_key2);

    String hi("hi");
    String hello("hello");
    String h("h");
    Column c_string('S', &kv);
    StringArray string_cache(5);
    string_cache.push(&hi);
    string_cache.push(&hello);
    string_cache.push(&hello);
    string_cache.push(&hi);
    string_cache.push(&h);
    temp_string.concat("_string");
    Key temp_key3(&temp_string, 0);
    c_string.push_back(&string_cache, &temp_key3);

    Column c_bool('B', &kv);
    BoolArray bool_cache(3);
    bool_cache.push((bool)0);
    bool_cache.push((bool)1);
    bool_cache.push((bool)1);
    temp_string.concat("_bool");
    Key temp_key4(&temp_string, 0);
    c_bool.push_back(&bool_cache, &temp_key4);

    // We removed adding columns, so here's a disgusting juryrig
    ColumnArray* new_cols = new ColumnArray(4);
    new_cols->push(&c_int);
    new_cols->push(&c_double);
    new_cols->push(&c_string);
    new_cols->push(&c_bool);
    delete df.cols_;
    df.cols_ = new_cols;
    df.schema_.num_cols_ = 4;
    df.schema_.num_rows_ = 5;

    assert(df.get_schema().width() == 4);
    assert(df.get_schema().length() == 5);

    char* serial = df.serialize();
    Deserializer deserializer(serial);
    DataFrame* deserial_df = new DataFrame(deserializer, &kv);

    assert(deserial_df->get_schema().width() == 4);
    assert(deserial_df->get_schema().length() == 5);

    assert(deserial_df->get_int(0, 0) == 1);
    assert(deserial_df->get_int(0, 1) ==3);
    assert(deserial_df->get_int(0, 2) == 4);
    assert(deserial_df->get_int(0, 3) == 2);

    assert(deserial_df->get_double(1, 0) == (double)1.2);
    assert(deserial_df->get_double(1, 1) == (double)3.2);
    assert(deserial_df->get_double(1, 2) == (double)2);
    assert(deserial_df->get_double(1, 3) == (double)1);

    assert(deserial_df->get_string(2, 0)->equals(&hi));
    assert(deserial_df->get_string(2, 1)->equals(&hello));
    assert(deserial_df->get_string(2, 2)->equals(&hello));
    assert(deserial_df->get_string(2, 3)->equals(&hi));
    assert(deserial_df->get_string(2, 4)->equals(&h));

    assert(deserial_df->get_bool(3, 0) == false);
    assert(deserial_df->get_bool(3, 1) == 1);
    assert(deserial_df->get_bool(3, 2) == true);

    // Now we aren't checking that Columns have the same length, so here's a test for it
    assert(deserial_df->cols_->get(0)->size() < df.get_schema().length());
    assert(deserial_df->cols_->get(1)->size() < df.get_schema().length());
    assert(deserial_df->cols_->get(3)->size() < df.get_schema().length());

    delete[] serial;
    delete deserial_df;
    printf("DataFrame basic serialization complete!\n");
}

// TODO: Cristian, bring this back
// void test_complex_dataframe() {
//     size_t local_node_index = 18;
//     // This test only works with a completely local kv, no distribution
//     Key dataframe_key("mainframe", local_node_index);

//     KV_Store kv(local_node_index);
//     size_t column_index = 0;
//     Column string_column('S', &kv, dataframe_key.get_key(), column_index++);
//     Column double_column('D', &kv, dataframe_key.get_key(), column_index++);
//     Column bool_column('B', &kv, dataframe_key.get_key(), column_index++);
//     Column int_column('I', &kv, dataframe_key.get_key(), column_index++);

//     size_t buffered_elements_size = 83;
//     size_t number_of_kv_chunks = 4;
//     size_t string_column_count = ELEMENT_ARRAY_SIZE * number_of_kv_chunks + buffered_elements_size;
//     String base_string("col_s7r1ng_");
//     double double_decimal = 0.012;
//     for (size_t ii = 0; ii < string_column_count; ii++) {
//         String temp_string(base_string);
//         temp_string.concat(ii);
//         string_column.push_back(&temp_string);
//         double_column.push_back(ii + double_decimal);
//         int_column.push_back((int)ii);
//         bool_column.push_back(true);
//     }
//     assert(string_column.keys_->length() == number_of_kv_chunks + 1);
//     assert(double_column.keys_->length() == number_of_kv_chunks + 1);
//     assert(int_column.keys_->length() == number_of_kv_chunks + 1);
//     assert(bool_column.keys_->length() == number_of_kv_chunks + 1);

//     String c("main");
//     Schema s1("");
//     DataFrame df(s1, &c, &kv);
//     df.add_column(&int_column);
//     df.add_column(&double_column);
//     df.add_column(&string_column);
//     df.add_column(&bool_column);
    
//     assert(df.get_schema().width() == 4);
//     assert(df.get_schema().length() == string_column_count);

//     char* serial = df.serialize();
//     Deserializer deserializer(serial);
//     DataFrame* deserial_df = new DataFrame(deserializer, &kv);

//     assert(deserial_df->get_schema().width() == 4);
//     assert(deserial_df->get_schema().length() == string_column_count);

//     for (size_t ii = 0; ii < string_column_count; ii++) {
//         assert(deserial_df->get_int(0, ii) == ii);
//         assert(deserial_df->get_double(1, ii) == ii + double_decimal);
//         String temp_string(base_string);
//         temp_string.concat(ii);
//         assert(deserial_df->get_string(2, ii)->equals(&temp_string));
//         assert(deserial_df->get_bool(3, ii));
//     }

//     delete[] serial;
//     delete deserial_df;
//     printf("DataFrame complex serialization complete!\n");
// }

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
    String* string_value_2 = new String(deserial);
    assert(string_value->size() == string_value_2->size());
    assert(strcmp(string_value->c_str(), string_value_2->c_str()) == 0);

    delete[] serial;
    delete string_value;
    delete string_value_2;
    printf("General serializing passed!\n");
}

void serialize_equals_test() {
    size_t size_t_value = 55;
    String string_value("hhihihi");

    size_t serial_len = sizeof(size_t) + sizeof(size_t) + string_value.serial_len();
    Serializer serializer1(serial_len);
    Serializer serializer2(serial_len);
    Serializer serializer3(serial_len);

    serializer1.serialize_size_t(size_t_value);
    serializer1.serialize_object(&string_value);
    serializer2.serialize_size_t(size_t_value);
    serializer2.serialize_object(&string_value);
    // This will show that order is important for a serializer
    serializer3.serialize_object(&string_value);
    serializer3.serialize_size_t(size_t_value);

    assert(serializer1.equals(&serializer1));
    assert(serializer1.equals(&serializer2));
    assert(!serializer1.equals(&serializer3));
    assert(serializer2.equals(&serializer1));
    assert(serializer2.equals(&serializer2));
    assert(!serializer2.equals(&serializer3));
    assert(!serializer3.equals(&serializer1));
    assert(!serializer3.equals(&serializer2));
    assert(serializer3.equals(&serializer3));
    printf("Serializer equals passed!\n");
}

void serialize_clone_test() {
    String* string1 = new String("A proper sentence.\n");
    size_t size_of_array = 1;
    StringArray* string_array = new StringArray(size_of_array);
    string_array->push(string1);
    Serializer* serial1 = new Serializer(string_array->serial_len());
    serial1->serialize_object(string_array);

    Serializer* serial_clone = serial1->clone();
    assert(serial1->serial_size_ == serial_clone->serial_size_);
    assert(serial1->serial_index_ == serial_clone->serial_index_);
    assert(strncmp(serial1->serial_, serial_clone->serial_, serial1->serial_size_) == 0);
    delete string1;
    delete string_array;
    delete serial1;

    char* char_serial = serial_clone->get_serial();
    Deserializer deserialize(char_serial);
    StringArray* string_array_clone = new StringArray(deserialize);
    String* string_clone = string_array_clone->get(0);
    String temp_string("A proper sentence.\n");
    assert(string_clone->equals(&temp_string));
    delete serial_clone;
    delete[] char_serial;
    delete string_array_clone;

    printf("Serializer clone passed!\n");
}

int main(int argc, char const *argv[]) 
{   
    serializing_test();
    test_string();
    test_bool_array();
    test_double_array();
    test_int_array();
    test_string_array();
    test_key();
    test_ack();
    test_put();
    test_get();
    test_wait_get();
    test_value();
    test_directory();
    test_kill();
    test_register();
    test_schema();
    test_int_column();
    test_double_column();
    test_bool_column();
    test_string_column();
    test_column_array();
    test_basic_dataframe(); 
    // test_complex_dataframe();
    serialize_equals_test();
    serialize_clone_test();
    printf("All tests passed!\n");
    return 0;
} 