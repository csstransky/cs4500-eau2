#include "object.h"

union Payload {
  int i;
  double d;
  bool b;
  Object* o;
};

Payload int_to_payload_(int int_value) { 
  Payload payload;
  payload.i = int_value;
  return payload;
}

Payload double_to_payload_(double double_value) {
  Payload payload;
  payload.d = double_value;
  return payload;
}

Payload bool_to_payload_(bool bool_value) {
  Payload payload;
  payload.b = bool_value;
  return payload;
}

Payload object_to_payload_(Object* object_value) {
  Payload payload;
  payload.o = object_value;
  return payload;
}