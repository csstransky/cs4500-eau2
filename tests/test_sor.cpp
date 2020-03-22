// Made by Kaylin Devchand and Cristian Stransky

#include "../src/file_adapter/sor.h"
#include <assert.h>

void test_file(char* file_path, size_t file_columns, size_t file_rows) {
    KV_Store kv(0);
    String name("data");
    SoR sor(file_path, &name, &kv);
    DataFrame* dataframe = sor.get_dataframe();
    Schema schema = dataframe->get_schema();

    assert(schema.length() == file_rows);
    assert(schema.width() == file_columns);

    for (size_t ii = 0; ii < file_columns; ii++) {
        char col_type = schema.col_type(ii);
        for (size_t jj = 0; jj < file_rows; jj++) {
            switch (col_type) {
                case 'I': {
                    int int_value = dataframe->get_int(ii, jj);
                    assert(int_value || int_value == 0);
                    break;
                }
                case 'B': {
                    bool bool_value = dataframe->get_bool(ii, jj);
                    // kind of a useless test with bools really
                    assert(bool_value || !bool_value);
                    break;
                }
                case 'F': {
                    assert(dataframe->get_float(ii, jj));
                    break;
                }
                case 'S': {
                    // We want to make sure the String isn't a nullptr
                    String* string_value = dataframe->get_string(ii, jj);
                    assert(string_value == nullptr || string_value->c_str());
                    assert(string_value == nullptr 
                        || string_value->size() || string_value->size() == 0);
                    break;
                }
            }
        }
    }

    delete dataframe;
}

void test_easy_txt(char* file_path) {
    KV_Store kv(0);
    String name("data");
    SoR sor(file_path, &name, &kv);
    DataFrame* dataframe = sor.get_dataframe();

    char* schema_types = dataframe->get_schema().types_;
    assert(strcmp("SFIBS", schema_types) == 0);

    String string1("hi");
    assert(dataframe->get_string(0, 0)->equals(&string1));
    assert(dataframe->get_float(1, 0) == (float)12.34);
    assert(dataframe->get_int(2, 0) == 22);
    assert(!dataframe->get_bool(3, 0));
    String string2("true");
    assert(dataframe->get_string(0, 0)->equals(&string1));

    delete dataframe;
}

void test_doc_txt(char* file_path) {
    KV_Store kv(0);
    String name("data");
    SoR sor(file_path, &name, &kv);
    DataFrame* dataframe = sor.get_dataframe();

    char* schema_types = dataframe->get_schema().types_;
    assert(strcmp("SISSBBBB", schema_types) == 0);
    
    String string0("0hi");
    assert(dataframe->get_string(0, 0)->equals(&string0));
    assert(dataframe->get_int(1, 0) == 11);
    String string1("true");
    assert(dataframe->get_string(2, 0)->equals(&string1));
    assert(dataframe->get_string(3, 0)->equals(&DEFAULT_STRING_VALUE));
    assert(!dataframe->get_bool(4, 0));
    assert(!dataframe->get_bool(5, 0));
    assert(!dataframe->get_bool(6, 0));
    assert(!dataframe->get_bool(7, 0));

    String string2("1yellow");
    assert(dataframe->get_string(0, 1)->equals(&string2));
    assert(dataframe->get_int(1, 1) == 10);
    String string3("false");
    assert(dataframe->get_string(2, 1)->equals(&string3));
    String string4("string");
    assert(dataframe->get_string(3, 1)->equals(&string4));
    assert(!dataframe->get_bool(4, 1));
    assert(!dataframe->get_bool(5, 1));
    assert(!dataframe->get_bool(6, 1));
    assert(!dataframe->get_bool(7, 1));

    String string5("212");
    assert(dataframe->get_string(0, 2)->equals(&string5));
    assert(dataframe->get_int(1, 2) == 0);
    String string6("true");
    assert(dataframe->get_string(2, 2)->equals(&string6));
    String string6_2("");
    assert(dataframe->get_string(3, 2)->equals(&string6_2));
    assert(!dataframe->get_bool(4, 2));
    assert(!dataframe->get_bool(5, 2));
    assert(!dataframe->get_bool(6, 2));
    assert(!dataframe->get_bool(7, 2));

    String string7("3vroom");
    assert(dataframe->get_string(0, 3)->equals(&string7));
    assert(dataframe->get_int(1, 3) == 172);
    String string8("hi");
    assert(dataframe->get_string(2, 3)->equals(&string8));
    String string9("hihi");
    assert(dataframe->get_string(3, 3)->equals(&string9));
    assert(!dataframe->get_bool(4, 3));
    assert(!dataframe->get_bool(5, 3));
    assert(!dataframe->get_bool(6, 3));
    assert(!dataframe->get_bool(7, 3));
    
    String string10("4hello");
    assert(dataframe->get_string(0, 4)->equals(&string10));
    assert(dataframe->get_int(1, 4) == 12);
    String string11("false");
    assert(dataframe->get_string(2, 4)->equals(&string11));
    String string12("");
    assert(dataframe->get_string(3, 4)->equals(&string12));
    assert(dataframe->get_bool(4, 4));
    assert(!dataframe->get_bool(5, 4));
    assert(!dataframe->get_bool(6, 4));
    assert(!dataframe->get_bool(7, 4));

    String string13("5\"quote checl\"");
    assert(dataframe->get_string(0, 5)->equals(&string13));
    assert(dataframe->get_int(1, 5) == 0);
    String string14("\"I guess this'll\"worktoo");
    assert(dataframe->get_string(2, 5)->equals(&string14));
    assert(dataframe->get_string(3, 5)->equals(&DEFAULT_STRING_VALUE));
    assert(!dataframe->get_bool(4, 5));
    assert(!dataframe->get_bool(5, 5));
    assert(!dataframe->get_bool(6, 5));
    assert(!dataframe->get_bool(7, 5));

    String string16("");
    assert(dataframe->get_string(0, 6)->equals(&string16));
    assert(dataframe->get_int(1, 6) == 0);
    String string17("reallyno");
    assert(dataframe->get_string(2, 6)->equals(&string17));
    assert(dataframe->get_string(3, 6)->equals(&DEFAULT_STRING_VALUE));
    assert(!dataframe->get_bool(4, 6));
    assert(!dataframe->get_bool(5, 6));
    assert(!dataframe->get_bool(6, 6));
    assert(!dataframe->get_bool(7, 6));

    delete dataframe;
}

int main(int argh, char** argv) {
    char* easy_txt = const_cast<char*>("../data/easy.txt");
    char* doc_txt = const_cast<char*>("../data/doc.txt");

    test_file(easy_txt, 5, 1);
    test_file(doc_txt, 8, 7);
    test_easy_txt(easy_txt);
    test_doc_txt(doc_txt);
    printf("SoR Tests Complete!\n");
    return 0;
} 