// Made by Kaylin Devchand and Cristian Stransky

#include "../src/helpers/string.h"

void print_serial_section(char* serial_array, size_t start_index, size_t end_index) {
    for (start_index; start_index < end_index; start_index++) {
        printf("%d\n", serial_array[start_index]);
    }
}

void int_example() {
    printf("First a basic serialization of an int value:\n");
    int int_value = 294747;
    printf("int_value: %d\n", int_value);

    printf("We convert the integer into a byte stream that will output the following:\n\n");
    size_t int_serial_length = sizeof(size_t) + sizeof(int);
    Serializer serializer(int_serial_length);
    serializer.serialize_int(int_value);

    char* int_serial = serializer.get_serial();
    printf("The first 8 bytes of every serial byte stream is how long the entire stream is (including these bytes)\n");
    size_t start_index = 0;
    size_t end_index = sizeof(size_t);
    print_serial_section(int_serial, start_index, end_index);

    printf("Then the int value will be represented in a byte stream as follows\n");
    start_index = end_index;
    end_index += sizeof(int);
    print_serial_section(int_serial, start_index, end_index);
    
    printf("\nDeserializing reads those byte streams to get values (MUST BE DONE IN ORDER AS SERIALIZED!):\n");
    Deserializer deserializer(int_serial);
    size_t serial_length = deserializer.deserialize_size_t();
    int int_value_deserial = deserializer.deserialize_int();
    printf("serial_length: %zu\n", serial_length);
    printf("int_value: %d\n\n", int_value_deserial);

    delete[] int_serial;
}

void string_example() {
    printf("Now a serialization of a String\n");
    String string_value("Hi There.");
    
    printf("Each Class that inherits from an Object has its own serialize function that makes serializing dynamic\n\n");
    char* string_serial = string_value.serialize();

    printf("The first 8 bytes of every serial byte stream is how long the entire stream is (including these bytes)\n");
    size_t start_index = 0;
    size_t end_index = sizeof(size_t);
    print_serial_section(string_serial, start_index, end_index);

    printf("Then is the total number of characters in the char array (EXCLUDING the null terminator)\n");
    start_index = end_index;
    end_index += sizeof(size_t);
    print_serial_section(string_serial, start_index, end_index);

    printf("Then is the char array itself\n");
    start_index = end_index;
    end_index += sizeof(char) * (string_value.size() + 1);
    print_serial_section(string_serial, start_index, end_index);

    printf("\nDeserializing is done through static methods specific to each Class:\n");
    String* string_deserial = String::deserialize(string_serial);
    printf("size: %zu\n", string_deserial->size());
    printf("char array: \"%s\"\n\n", string_deserial->c_str());

    delete[] string_serial;
    delete string_deserial;
}

int main(int argc, char const *argv[]) 
{   
    int_example();
    string_example();
    printf("For more examples, visit the test_serial.cpp file.\n\n");
    return 0;
} 