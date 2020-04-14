// Made by Kaylin Devchand and Cristian Stransky
//lang::C++
#pragma once

#include <vector>
#include <algorithm>
#include <fstream>
#include <iostream>

#include "../helpers/helper.h"
#include "../helpers/string.h"
#include "../helpers/array.h"
#include "../dataframe/dataframe.h"
#include "../dataframe/dataframe_builder.h"

using namespace std;

enum enum_type {INTEGER, DOUBLE, BOOL, STRING, EMPTY};

// Assignment only wants us to read the first 500 lines to find the schema of a file
size_t MAX_SCHEMA_READ = 500;

// Creates a DataFrame by reading a formatted SoR file
// NOTE: The DataFrame is NOT deleted, and must be grabbed and deleted independently
class SoR {
    private:
    String* get_string_from_line(StringArray* line, size_t index) {
        if (index < line->length())
            return line->get(index);
        else
            return &DEFAULT_STRING_VALUE;
    }

    double get_double_from_line(StringArray* line, size_t index) {
        if (index < line->length()) {
            String* element = line->get(index);
            return stof(element->c_str());
        }
        else
            return DEFAULT_DOUBLE_VALUE;
    }

    int get_int_from_line(StringArray* line, size_t index) {
        if (index < line->length()) {
            String* element = line->get(index);
            return atoi(element->c_str());
        }
        else
            return DEFAULT_INT_VALUE;
    }

    bool get_bool(String* element) {
        return element->size() != 0 && element->at(0) != '0' && element->at(0) != 0;
    }

    bool get_bool_from_line(StringArray* line, size_t index) {
        if (index < line->length()) {
            String* element = line->get(index);
            return get_bool(element);
        }
        else
            return DEFAULT_BOOL_VALUE;
    }

    void add_line(StringArray* line) {
        Schema schema(cols_types_);
        Row row(schema);
        for (int i = 0; i < schema.width(); i++) {
            char type = schema.col_type(i);
            // NOTE: There is no checking to make sure that a value being input into the Row's 
            // Column is correct, it will cast whatever it is given. Example:
            // column_types = <STRING><INT>
            // <dude><3>
            // <hihi><there>
            // row[1] = dude, 3
            // row[2] = hihi, 0
            switch (type) {
                case 'S': {
                    String* string_value = get_string_from_line(line, i);
                    row.set(i, string_value);
                    break;
                }
                case 'D': {
                    double double_value = get_double_from_line(line, i);
                    row.set(i, double_value);
                    break;
                }
                case 'I': {
                    int int_value = get_int_from_line(line, i);
                    row.set(i, int_value);
                    break;
                }
                case 'B': {
                    bool bool_value = get_bool_from_line(line, i);
                    row.set(i, bool_value);
                    break;
                }
                default: {
                    // This should never be reached since the get_column_types() should never have
                    // a character other than "SFIB"
                    assert(0);
                }
            }
        }
        df_builder_->add_row(row);
    }

    bool is_file_boolean(String* line_string) {
        return strcmp(line_string->c_str(), "0") == 0 || strcmp(line_string->c_str(), "1") == 0;
    }

    bool is_first_char_numeric(String* line_string) {
        // Ints and doubles can start with a + or - character, like "-2" or "+30"
        return line_string->size() > 0 && (line_string->at(0) == '+' || line_string->at(0) == '-' || isdigit(line_string->at(0)));
    }

    bool is_file_double(String* line_string) {
        if (!is_first_char_numeric(line_string)) 
            return false;

        // We only consider a double to have ONE dot, ex: "12.2" = True, "1.1.1" = False, "13" = False
        bool has_dot = false;
        for (size_t ii = 1; ii < line_string->size(); ii++) {
            if (line_string->at(ii) == '.' && !has_dot)
                has_dot = true;
            else if (!isdigit(line_string->at(ii)) || (line_string->at(ii) == '.' && has_dot))
                return false;
        }
        return has_dot;
    }

    bool is_file_int(String* line_string) {
        if (!is_first_char_numeric(line_string)) 
            return false;

        for (size_t ii = 1; ii < line_string->size(); ii++) {
            if (!isdigit(line_string->at(ii)))
                return false;
        }
        return true;
    }

    bool is_file_string(String* line_string) {
        // A file_string can NOT have spaces, UNLESS they are spaces within quotes, ex: '  hu ' = False, '"hi there"' = True
        // A string with nothing is not considered a string either, ex: '' = False, '""' = True
        if (line_string->size() <= 0) 
            return false;

        bool is_quotes = false;
        for (size_t ii = 0; ii < line_string->size(); ii++) {
            if (line_string->at(ii) == ' ' && !is_quotes)
                return false;
            else if (line_string->at(ii) == '\"')
                is_quotes = !is_quotes;
        }
        return true;
    }

    char get_column_char(String* line_string) {
        // NOTE: Order of these if else statements is important!
        if (is_file_double(line_string)) return 'D';
        // NOTE: Because of this, a "1" and "0" integer is NOT possible
        else if (is_file_boolean(line_string)) return 'B';
        else if (is_file_int(line_string)) return 'I';
        else if (is_file_string(line_string)) return 'S';
        // If a file input cannot match into any category, they will be considred a Bool
        else return 'B';
    }

    char* convert_strings_to_column_types(StringArray* column_strings) {
        char* column_types = new char[column_strings->length() + 1];

        for (size_t ii = 0; ii < column_strings->length(); ii++) {
            char col_enum = get_column_char(column_strings->get(ii));
            column_types[ii] = col_enum;
        }
        column_types[column_strings->length()] = '\0';
        return column_types;
    }

    StringArray* get_max_column_strings(ifstream& in_file) {
        String file_line_string("");
        char file_char;
        bool is_record = false;
        bool is_quotes = false;
        StringArray* max_column_strings = new StringArray();
        StringArray current_column_strings;
        int num_lines = 0;

        while(!in_file.eof() && num_lines < MAX_SCHEMA_READ) {
            in_file >> noskipws >> file_char;
            switch (file_char) {
                case ' ':
                    if (is_quotes && is_record) 
                        file_line_string.concat(file_char);
                    break;
                case '\n':
                    if (max_column_strings->length() <= current_column_strings.length()) {
                        delete max_column_strings;
                        max_column_strings = current_column_strings.clone();
                    }
                    current_column_strings.clear();
                    num_lines++;
                    break;
                case '\"':
                    if (is_record) {
                        is_quotes = !is_quotes;
                        file_line_string.concat(file_char);
                    }
                    break;
                case '<':
                    is_record = true;
                    break;
                case '>':
                    // I put this check here to make sure we have a pair of <>, to avoid the 
                    // case of "> dude >", which in our case will completely ignore it
                    if (is_record) {
                        current_column_strings.push(&file_line_string);

                        is_record = false;
                        file_line_string.clear(); 
                    }
                    is_quotes = false;
                    break;
                default:
                    if (is_record)
                        file_line_string.concat(file_char);
            }
        }
        return max_column_strings;
    }

    char* get_column_types(char* file_path) {
        // This function determines the column types by the FIRST INSTANCE of the largest column,
        // example:
        // <dello> <dog>
        // <12> <23>
        // output = <STRING> <STRING>
        char* column_types;
        ifstream in_file;
        in_file.open(file_path);
        column_types = nullptr;
        
        if (in_file.is_open()) {
            StringArray* max_column_strings = get_max_column_strings(in_file);
            column_types = convert_strings_to_column_types(max_column_strings);
            delete max_column_strings;
            in_file.close(); 
        }
        else {
            cout << "~ERROR: FILE NOT FOUND~\n";
        }
        return column_types;
    }

    void move_file_index_next_line(ifstream& in_file, size_t from, size_t len) {
        // move until new line
        char file_char;
        size_t end_byte = from + len;
        while(!in_file.eof() && end_byte > in_file.tellg()) {
            in_file >> noskipws >> file_char;
            if (file_char == '\n')
                return;
        }
    }

    void parse_and_add_file(ifstream& in_file, size_t from, size_t len) {
        String file_line_string("");
        char file_char;
        bool is_record = false;
        bool is_quotes = false;
        size_t end_byte = from + len;
        StringArray current_line;

        while(!in_file.eof() && end_byte > in_file.tellg()) {
            in_file >> noskipws >> file_char;
            switch (file_char) {
                case ' ':
                    if (is_quotes && is_record)
                        file_line_string.concat(file_char);
                    break;
                case '\n':
                    add_line(&current_line);
                    current_line.clear();
                    break;
                case '\"':
                    if (is_record) {
                        is_quotes = !is_quotes;
                        file_line_string.concat(file_char);
                    }
                    break;
                case '<':
                    is_record = true;
                    break;
                case '>':
                    // I put this check here to make sure we have a pair of <>, to avoid the 
                    // case of "> dude >", which in our case will completely ignore it
                    if (is_record) {
                        is_record = false;
                        current_line.push(&file_line_string);
                        file_line_string.clear();
                    }
                    is_quotes = false;
                    break;
                default:
                    if (is_record) {
                        file_line_string.concat(file_char);
                    }
            }
        }
    }

    void parse_and_add(char* file_path, size_t from, size_t len) {
        // Build the data structure using "from" and "len"
        ifstream in_file;
        in_file.open(file_path);
        if (in_file.is_open()) {
            // move to from position
            in_file.seekg(from, ios_base::beg);
            if (from > 0) {
                move_file_index_next_line(in_file, from, len);
            }

            parse_and_add_file(in_file, from, len); 
            in_file.close(); 
        }
        else {
            cout << "~ERROR: FILE NOT FOUND~\n";
        }
    }

    size_t get_file_size(char* path) {
        FILE* file = fopen(path, "rb");
        size_t file_size = 0;
        if (file) {
            fseek(file, 0L, SEEK_END); // goes to the end of the file
            file_size = ftell(file); // gets the size
            fclose(file); 
        }
        return file_size;
    }

    public:

    DataFrameBuilder* df_builder_;
    char* cols_types_;

    SoR(char* file_path, String* name, KV_Store* kv) : SoR(file_path, 0, get_file_size(file_path), name, kv) { }

    SoR(char* file_path, size_t from, size_t len, String* name, KV_Store* kv) {
        // get column types of the first 500 lines (using max column)
        cols_types_ = get_column_types(file_path);    
        df_builder_ = new DataFrameBuilder(cols_types_, name, kv);
        // parse again to add each element
        parse_and_add(file_path, from, len);
    }

    ~SoR() {
        delete df_builder_;
        delete[] cols_types_;
    }

    /**
     * Simply get the dataframe from a SoR, this will transfer ownership to the return point.
     * NOTE: Make sure to delete this, as it is independent to the SoR deconstructor.
     */
    DataFrame* get_dataframe() {
        return df_builder_->done();
    }
};