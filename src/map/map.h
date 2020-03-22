#pragma once
#include "../helpers/object.h"
#include "../array/array.h"

// NOTE: based on how much data we will be dealing with in the future, this number can change to
// accommodate that. I chose this number initially just because I saw it in a Wikipedia article.
const size_t DEFAULT_BUCKET_SIZE = 256;

/**
 * Represents a key and value pair, to be mainly used for Map
 * 
 * NOTE: I talked to prof. Vitek directly about this, and he told me that normally we'd have a 
 * private sub class for Pair inside of Map (which is what I prefer as well), but because we aren't 
 * using private, we're stuck making Pair public like this as its own class.
 */ 
class Pair : public Object {
    public:
        Object* key;
        Object* value;

        Pair(Object* key, Object* value) {
            this->key = key->clone();
            this->value = value->clone();
        }

        Pair(Pair* p) {
            this->key = p->get_key()->clone();
            this->value = p->get_value()->clone();
        }

        ~Pair() {
            delete key;
            delete value;
        }

        Object* get_key() { return key; }

        Object* get_value() { return value; }

        void set_value(Object* value) { 
            delete this->value;
            this->value = value->clone(); 
        }

        bool equals(Object* other) {
            Pair* other_pair = dynamic_cast<Pair*>(other);                                  
            if (other_pair == nullptr) return false;
            return this->key->equals(other_pair->get_key()) 
                && this->value->equals(other_pair->get_value());
        }

        Object* clone() {
            return new Pair(this);
        }

        /**
         * Gets a size_t representation of a Pair.
         * 
         * To ensure that two pairs are truly different, we're using subtraction, example:
         * "hi"->hash() = 20
         * "there"->hash() = 40
         * Pair1 = {"hi", "there"} -> hash() = 20
         * Pair2 = {"there", "hi"} -> hash() = -20
         * 
         * NOTE: Because this is size_t, negatives instead will be represented by a very large num
         * 
         * @return size_t of the subtraction between value and key
         */
        size_t hash() {
            return value->hash() - key->hash();
        }
};

/**
 * Map - data structure that is to be used for our project which maps an Object to an Object
 */ 
class Map: public Object {
    public:
        size_t count_;
        ObjectArray* buckets_;
        size_t buckets_size_;

        /**
         * @brief Basic initialization of an empty Map
         */
        Map() {
            count_ = 0;
            buckets_size_ = DEFAULT_BUCKET_SIZE;
            buckets_ = new ObjectArray(buckets_size_);
            // We want a little more speed, so we're going to initialize all the bucket arrays by 
            // 1 element, just to avoid an initialization check every time we get and set
            ObjectArray temp_array(1);
            for(size_t ii = 0; ii < buckets_size_; ii++) {
                buckets_->push(&temp_array);
            }
        }

        /**
         * @brief Deletes a Map including all private data structures, such as buckets
         * NOTE: Objects inside the Map will NOT be freed
         */
        ~Map() {
            delete buckets_;
        }

        /**
         * @brief - Get the size of this map.
         * 
         * @return size_t - the size of this map.
         */
        virtual size_t size() {
            return count_;
        }

        /**
         * @brief - Is this map empty?
         * 
         * @return true - if this map is empty
         * @return false: if this map is not empty
         */
        virtual bool isEmpty() {
            return count_ == 0;
        }

        /**
         * @brief - Does this map contain key?
         * 
         * @param key - the key to search for
         * @return true - if the key exists in this map
         * @return false - if the key does not exist in this map
         */
        virtual bool containsKey(Object* key) {
            Object* value = get(key);
            return value != nullptr;
        }

        /**
         * @brief - Does this map contain value?
         * 
         * @param value - the value to search for
         * @return true - if the value exists in this map
         * @return false - if the value does not exist in this map
         */
        virtual bool containsValue(Object* value) {
            for (size_t ii = 0; ii < buckets_size_; ii++) {
                ObjectArray* bucket_array = dynamic_cast<ObjectArray*>(buckets_->get(ii));
                for (size_t jj = 0; jj < bucket_array->length(); jj++) {
                    Pair* pair = dynamic_cast<Pair*>(bucket_array->get(jj));
                    if (value->equals(pair->get_value())) {
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * @brief - Hashes the Object to be used inside of the buckets as an index
         * @param key - an Object
         * 
         * @return size_t - buckets index to be used to store the Object
         */
        virtual size_t hash_bucket_(Object* key) {
            return key->hash() % (buckets_size_ - 1);
        }

        /**
         * @brief - Get the value for the key.
         * If the key does not exist, return a nullptr.
         * 
         * @param key - the key to return the value for.
         * @return Object* - the value that corresponds to key
         */
        virtual Object* get(Object* key) {
            size_t bucket_index = hash_bucket_(key);
            ObjectArray* bucket_array = dynamic_cast<ObjectArray*>(buckets_->get(bucket_index));
            for (size_t ii = 0; ii < bucket_array->length(); ii++) {
                Pair* pair = dynamic_cast<Pair*>(bucket_array->get(ii));
                if (key->equals(pair->get_key())) {
                    return pair->get_value();
                }
            }
            return nullptr;
        }

        /**
         * @brief - Doubles the number of buckets by allocating a new arrays and deleting old arrays.
         */
        virtual void increase_map_size_() {
            size_t new_buckets_size_ = buckets_size_ * 2;
            ObjectArray* new_buckets = new ObjectArray(new_buckets_size_);
            for(size_t ii = 0; ii < new_buckets_size_; ii++) {
                ObjectArray o(1);
                new_buckets->push(&o);
            }
            ObjectArray* old_buckets = buckets_;
            size_t old_bucket_size = buckets_size_;
            buckets_ = new_buckets;
            buckets_size_ = new_buckets_size_;
            count_ = 0;

            // Because the bucket_size_ has changed, now the hashing function has changed, so every
            // single element in our old map needs to be rehashed again into the new map
            for (size_t ii = 0; ii < old_bucket_size; ii++) {
                ObjectArray* old_bucket_array = dynamic_cast<ObjectArray*>(old_buckets->get(ii));
                for (size_t jj = 0; jj < old_bucket_array->length(); jj++) {
                    Pair* pair = dynamic_cast<Pair*>(old_bucket_array->get(jj));
                    this->put(pair->get_key(), pair->get_value());
                }
            }
            delete old_buckets;
        }
        
        /**
         * @brief - Put the given key-value pair in this map.
         * 
         * @param key - the key to insert, cannot be null
         * @param value - the value to insert
         * @return Object* - the previous value for the given key if exists, else nullptr
         */
        virtual Object* put(Object* key, Object* value) {
            // We decided to increase the map whenever half of the buckets are filled up, to give
            // us better hashing and less collisions (bucket_size_ affects the hashing function).
            if (count_ >= (buckets_size_ / 2)) {
                increase_map_size_();
            }
            size_t bucket_index = hash_bucket_(key);
            ObjectArray* bucket_array = dynamic_cast<ObjectArray*>(buckets_->get(bucket_index));
            
            for (size_t ii = 0; ii < bucket_array->length(); ii++) {
                Pair* old_pair = dynamic_cast<Pair*>(bucket_array->get(ii));
                if (key->equals(old_pair->get_key())) {
                    Object* old_value = old_pair->get_value()->clone();
                    old_pair->set_value(value);
                    return old_value;
                }
            }

            Pair new_pair(key, value);
            bucket_array->push(&new_pair);
            count_++;
            return nullptr;
        }

        /**
         * @brief - Remove the key-value pair from this map.
         * 
         * @param key - the key to remove
         * @return Object* - the value of the key that was removed if exists, else nullptr
         */
        virtual Object* remove(Object* key) {
            size_t bucket_index = hash_bucket_(key);
            ObjectArray* bucket_array = dynamic_cast<ObjectArray*>(buckets_->get(bucket_index));


            for (size_t ii = 0; ii < bucket_array->length(); ii++) {
                Pair* pair = dynamic_cast<Pair*>(bucket_array->get(ii));
                if (key->equals(pair->get_key())) {
                    Object* old_value = pair->get_value()->clone();
                    bucket_array->remove(ii);
                    delete pair;
                    count_--;
                    return old_value;
                }
            }
            return nullptr;
        }

        /**
         * @brief - Put all the contents of other into this map.
         * 
         * @param other - the other map to load all the contents from
         */
        virtual void putAll(Map* other) {  
            ObjectArray* other_keys = dynamic_cast<ObjectArray*>(other->keySet());
            for (size_t ii = 0; ii < other_keys->length(); ii++) {
                Object* key = other_keys->get(ii);
                Object* value = other->get(key);
                this->put(key, value);
            }
            delete other_keys;
        }

        /**
         * @brief - Clear the contents of this map so it is empty.
         * 
         */
        virtual void clear() {
            // I'm choosing to clear the current buckets_ instead of creating a new buckets_ and
            // initializing it, to save on having to more new and delete functions.
            for (size_t ii = 0; ii < buckets_size_; ii++) {
                ObjectArray* bucket_array = dynamic_cast<ObjectArray*>(buckets_->get(ii));
                for (size_t jj = 0; jj < bucket_array->length(); jj++) {
                    Pair* pair = dynamic_cast<Pair*>(bucket_array->get(jj));
                    bucket_array->pop();
                    delete pair;
                }
            }
            count_ = 0;
        }

        /**
         * @brief - Get a list of the keys present in this map.
         * 
         * @return Array* - the list of keys present in this map.
         */
        virtual Array* keySet() {
            ObjectArray* keys = new ObjectArray(count_);
            for (size_t ii = 0; ii < buckets_size_; ii++) {
                ObjectArray* bucket_array = dynamic_cast<ObjectArray*>(buckets_->get(ii));
                for (size_t jj = 0; jj < bucket_array->length(); jj++) {
                    Pair* pair = dynamic_cast<Pair*>(bucket_array->get(jj));
                    keys->push(pair->get_key());
                }
            }
            return keys;
        }

        /**
         * @brief - Get a list of the values present in this map.
         * 
         * @return Array* - the list of values present in this map.
         */
        virtual Array* values() {
            ObjectArray* values = new ObjectArray(count_);
            for (size_t ii = 0; ii < buckets_size_; ii++) {
                ObjectArray* bucket_array = dynamic_cast<ObjectArray*>(buckets_->get(ii));
                for (size_t jj = 0; jj < bucket_array->length(); jj++) {
                    Pair* pair = dynamic_cast<Pair*>(bucket_array->get(jj));
                    values->push(pair->get_value());
                }
            }
            return values;
        }

        /**
         * @brief - Get the hash of this map.
         * 
         * @return size_t - the hash of this map
         */
        virtual size_t hash() {
            size_t total_hash = 0;
            for (size_t ii = 0; ii < buckets_size_; ii++) {
                ObjectArray* bucket_array = dynamic_cast<ObjectArray*>(buckets_->get(ii));
                for (size_t jj = 0; jj < bucket_array->length(); jj++) {
                    Pair* pair = dynamic_cast<Pair*>(bucket_array->get(jj));
                    total_hash += pair->hash();
                }
            }
            return total_hash;
        }
};

/**
 * @brief OSMap - Map from Object to String
 * 
 */
class OSMap : public Map {
    public:

        /**
         * @brief - Get the value for the key.
         * If the key does not exist, return a nullptr.
         * 
         * @param key - the key to return the value for.
         * @return String* - the value that corresponds to key
         */
        virtual String* get(Object* key) {
            return dynamic_cast<String*>(Map::get(key));
        }
        
        /**
         * @brief - Put the given key-value pair in this map.
         * 
         * @param key - the key to insert, cannot be null
         * @param value - the value to insert
         * @return String* - the previous value for the given key if exists, else nullptr
         */
        virtual String* put(Object* key, String* value) {
            return dynamic_cast<String*>(Map::put(key, value));
        }

        /**
         * @brief - Remove the key-value pair from this map.
         * 
         * @param key - the key to remove
         * @return String* - the value of the key that was removed if exists, else nullptr
         */
        virtual String* remove(Object* key) {
            return dynamic_cast<String*>(Map::remove(key));
        }

        /**
         * @brief - Does this map contain value?
         * 
         * @param value - the value to search for
         * @return true - if the value exists in this map
         * @return false - if the value does not exist in this map
         */
        virtual bool containsValue(String* value) {
            return Map::containsValue(value);
        }
};

/**
 * @brief SOMap - Map from String to Object
 * 
 */
class SOMap : public Map {
    public:

        /**
         * @brief - Does this map contain key?
         * 
         * @param key - the key to search for
         * @return true - if the key exists in this map
         * @return false - if the key does not exist in this map
         */
        bool containsKey(String* key) {
            return Map::containsKey(key);
        }

        /**
         * @brief - Get the value for the key.
         * If the key does not exist, return a nullptr.
         * 
         * @param key - the key to return the value for.
         * @return Object* - the value that corresponds to key
         */
        virtual Object* get(String* key) {
            return Map::get(key);
        }
        
        /**
         * @brief - Put the given key-value pair in this map.
         * 
         * @param key - the key to insert, cannot be null
         * @param value - the value to insert
         * @return Object* - the previous value for the given key if exists, else nullptr
         */
        virtual Object* put(String* key, Object* value) {
            return Map::put(key, value);
        }

        /**
         * @brief - Remove the key-value pair from this map.
         * 
         * @param key - the key to remove
         * @return Object* - the value of the key that was removed if exists, else nullptr
         */
        virtual Object* remove(String* key) {
            return Map::remove(key);
        }
};

/**
 * @brief SSMap - Map from String to String
 * 
 */
class SSMap : public Map {
    public:

        /**
         * @brief - Does this map contain value?
         * 
         * @param value - the value to search for
         * @return true - if the value exists in this map
         * @return false - if the value does not exist in this map
         */
        virtual bool containsValue(String* value) {
            return Map::containsValue(value);
        }

        /**
         * @brief - Get the value for the key.
         * If the key does not exist, return a nullptr.
         * 
         * @param key - the key to return the value for.
         * @return Object* - the value that corresponds to key
         */
        virtual String* get(String* key) {
            return dynamic_cast<String*>(Map::get(key));
        }
        
        /**
         * @brief - Put the given key-value pair in this map.
         * 
         * @param key - the key to insert
         * @param value - the value to insert
         * @return Object* - the previous value for the given key if exists, else nullptr
         */
        virtual String* put(String* key, String* value) {
            return dynamic_cast<String*>(Map::put(key, value));
        }

        /**
         * @brief - Remove the key-value pair from this map.
         * 
         * @param key - the key to remove
         * @return Object* - the value of the key that was removed if exists, else nullptr
         */
        virtual String* remove(String* key) {
            return dynamic_cast<String*>(Map::remove(key));
        }
};
