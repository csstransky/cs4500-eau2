#pragma once                                                                     
//lang::CwC                                                                      
#include "../helpers/object.h"                                                              

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
            this->key = key;
            this->value = value;
        }

        ~Pair() {}

        Object* get_key() { return key; }

        Object* get_value() { return value; }

        void set_value(Object* value) { this->value = value; }

        bool equals(Object* other) {
            Pair* other_pair = dynamic_cast<Pair*>(other);                                  
            if (other_pair == nullptr) return false;
            return this->key->equals(other_pair->get_key()) 
                && this->value->equals(other_pair->get_value());
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