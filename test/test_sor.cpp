// Made by Kaylin Devchand and Cristian Stransky

#include "../src/file_adapter/sor.h"
#include <assert.h>

void test_file(const char* file_path, size_t file_columns, size_t file_rows) {
    char* test_file = const_cast<char*>(file_path);
    
    SoR sor(test_file);
    DataFrame* dataframe = sor.get_dataframe();
    Schema schema = dataframe->get_schema();

    
    printf("ROWS: %zu\n", schema.length());
    printf("COLUMNS: %zu\n", schema.width());
    printf("SCHEMA: %s\n", schema.types_);
    assert(schema.length() == file_rows);
    assert(schema.width() == file_columns);
        dataframe->print();
    for (size_t ii = 0; ii < file_columns; ii++) {
        char col_type = schema.col_type(ii);
        for (size_t jj = 0; jj < file_rows; jj++) {
            printf("COL: %zu, ROW: %zu\n", ii, jj);
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

}

int main(int argh, char** argv) {
    test_file("../data/easy.txt", 5, 1);
    test_file("../data/doc.txt", 8, 7);
    return 0;
} 