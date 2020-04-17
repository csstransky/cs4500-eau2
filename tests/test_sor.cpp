// Made by Kaylin Devchand and Cristian Stransky

#include "../src/helpers/sor.h"
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
                    assert(dataframe->get_double(ii, jj));
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

    String* schema_types = dataframe->get_schema().types_;
    assert(strcmp("SDIBS", schema_types->c_str()) == 0);

    String string1("hi");
    assert(dataframe->get_string(0, 0)->equals(&string1));
    assert(dataframe->get_double(1, 0) - ((double)12.34) < 0.001);
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

    String* schema_types = dataframe->get_schema().types_;
    assert(strcmp("IS", schema_types->c_str()) == 0);

    for (size_t ii = 0; ii < 5; ii++) {
        assert(dataframe->get_int(0, ii) == ii);
    }
    
    String string0("0xnoone");
    assert(dataframe->get_string(1, 0)->equals(&string0));

    String string1("tosch");
    assert(dataframe->get_string(1, 1)->equals(&string1));

    String string2("jmettraux");
    assert(dataframe->get_string(1, 2)->equals(&string2));

    String string3("SMGNMSKD");
    assert(dataframe->get_string(1, 3)->equals(&string3));

    String string4("kennethkalmer");
    assert(dataframe->get_string(1, 4)->equals(&string4));

    delete dataframe;
}

int main(int argh, char** argv) {
    char* easy_txt = const_cast<char*>("data/easy.txt");
    char* doc_txt = const_cast<char*>("data/doc.txt");

    test_file(easy_txt, 5, 1);
    test_file(doc_txt, 2, 5);
    test_easy_txt(easy_txt);
    test_doc_txt(doc_txt);
    printf("SoR Tests Complete!\n");
    return 0;
} 