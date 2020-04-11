#pragma once

#include "dataframe.h"

class DataFrameBuilder {
    public:
    String* name_;
    DataFrame* df_; // not owned
    ObjectArray buffers_;
	size_t num_nodes_;
	size_t num_chunks_;

	// TODO: Change the schema to an actual Schema object
    DataFrameBuilder(const char* schema, String* name, KV_Store* kv) : buffers_(strlen(schema)) {
        name_ = name->clone();
        Schema s(schema);
        df_ = new DataFrame(s, kv);
		num_nodes_ = kv->get_num_other_nodes();
		num_chunks_ = 0;

        for (size_t ii = 0; ii < s.width(); ii++) {
			Array* array;
            switch(s.col_type(ii)) {
                case 'I': array = new IntArray(); break;
                case 'D': array = new DoubleArray(); break;
                case 'B': array = new BoolArray(); break;
                case 'S': array = new StringArray(); break;
            }
			buffers_.push(array);
			delete array;
        }
    }

	~DataFrameBuilder() {
		delete name_;
	}

	Key* generate_key_(size_t column_index) {
		String key_name(*name_);
		key_name.concat('_');
		key_name.concat(column_index);
		key_name.concat('_');
		key_name.concat(num_chunks_);

		size_t home_index = num_chunks_ % num_nodes_;
		Key* new_key = new Key(&key_name, home_index);
		return new_key;    
  	}

	void add_to_column_() {
		for (size_t ii = 0; ii < buffers_.length(); ii++) {
			Array* array = static_cast<Array*>(buffers_.get(ii));
			if (array->length() > 0) {
				Key* key = generate_key_(ii);
				df_->get_column(ii)->push_back(array, key);
				delete key;
				array->clear();
			}
		}
		num_chunks_++;
	}

	bool is_buffer_full_() {
		size_t buffer_length = static_cast<Array*>(buffers_.get(0))->length();
		if (buffer_length > ELEMENT_ARRAY_SIZE)
			assert(0);
		return buffer_length == ELEMENT_ARRAY_SIZE;
	}
	
	void add_row(Row& row) {
		for (size_t ii = 0; ii < buffers_.length(); ii++) {
			switch(row.col_type(ii)) {
                case 'I': static_cast<IntArray*>(buffers_.get(ii))->push(row.get_int(ii)); break;
                case 'D': static_cast<DoubleArray*>(buffers_.get(ii))->push(row.get_double(ii)); break;
                case 'B': static_cast<BoolArray*>(buffers_.get(ii))->push(row.get_bool(ii)); break;
                case 'S': static_cast<StringArray*>(buffers_.get(ii))->push(row.get_string(ii)); break;
            }
		}
		if (is_buffer_full_()) 
			add_to_column_();
		df_->get_schema().add_row();
	}	

	DataFrame* done() {
		add_to_column_();
		return df_;
	}

};