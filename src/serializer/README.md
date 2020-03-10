# Serialization API Design
+ Kaylin Devchand
+ Cristian Stransky
## Serializer
We serialize by creating byte stream representations of primitive types and Classes, which are mainly created with the  `memcpy(...)` function. Deserializing is the same as serializing, but in *reverse* (order is important here and will discussed further later).

In order to make this easier on the user, we have created a Serializer class and a Deserializer class (both in serial.h, with the philosophy that a serializer MUST have a deserializer). Both of these classes focus on abstracting the rolling and unrolling of serials to allow for an easier experience.

Imagine we wish to create a serialized version of a class that holds a Boolean, integer, and String (or we simply wish to couple them similarly to a tuple), we would first create a Serializer. A Serializer mainly keeps track of the char array and its current index, which means **calling `serialize_***(...)` methods in order will progress the Serializer's char array**. The order is very important, and will dictate the construction of your serial. 

Below is an example of serializing values you wish to deserialize later (also a good example of what is usually inside an Object's `serialize()` method):
```
// Serializing String, Boolean, and Integer values into a byte stream
bool bool_value = true;
int int_value = 67;
String string_value("string");

size_t serial_length = sizeof(size_t) 
    + sizeof(bool) 
    + sizeof(int) 
    + string.serial_len();
Serializer serializer(serial_length);

// All our serials have the total serial length at the front
Serializer.serialize_size(serial_length); 
serializer.serialize_bool(bool_value);
serializer.serialize_int(int_value);
serializer.serialize_object(&string_value);
char* serialized_char_array = serializer.get_serial();

// NOTE: the Serializer deconstructor doesn't delete the char array, 
// that must be done by the user themselves.
```
**IMPORTANT:** Each Object MUST have a `serialize()` and `serial_len()` function to allow for `.serialize_object(...)` to dynamically serialize whatever Object you pass in (in most examples and tests here, that will be a String or Message).

Below is what our `serialized_char_array` will look like:
```
| 36|  0|  0|  0|  0|  0|  0|  0| - total serial size
|  1|                             - bool_value
| 67|  0|  0|  0|                 - int_value
| 23|  0|  0|  0|  0|  0|  0|  0| - String's total serial size
|  6|  0|  0|  0|  0|  0|  0|  0| - String's size (excluding null terminator)
|115|116|114|105|110|103|  0|     - String's char array
```

In literal form:
```
360000000167000230000000600000001151161141051101030
```
## Deserializer

Deserializing is exactly the same as serializing, except in reverse. It's very important to note: **there is no way to dynamically read from a serial, you MUST know the order of contents inside.** We followed this philosophy because we assume that every created serial will NEED a paired deserial. Polymorphism of classes is not supported, and instead *something* must be added to the serial to be able to differentiate (like `MsgKind` in `Message` for example). This also means in order to deserialize a class, a static `deserialize(Deserializer& deserializer)` must be added that will create the desired class from the current index inside the deserializer.

Take our previous serialized char array, deserializing the values out of it occurs as below:

```
// IMPORTANT: Deserializing MUST be done IN ORDER of serialization

Deserializer deserializer(serialized_char_array);
size_t serial_length = deserializer.deserialize_size_t();
bool bool_value = deserializer.deserialize_bool();
int int_value = deserializer.deserialize_int();
String* string_value = String::deserialize(deserializer);

// We cannot dynamically deserialize a Class, static Class methods are used instead
```
Make sure to keep track of the **order** and **type** of serializing, so that you can properly deserialize. Attempting to deserialize without care of order and type will horribly break things. 

If you simply wish to get the `int_value` from the example above, you MUST STILL deserialize in order to get to it.

The incorrect method:
```
Deserializer deserializer(serialized_char_array);
// WRONG, your int value will instead be the total serial length
int int_value = deserializer.deserialize_int(); // DO NOT DO; BAD
```
The correct method:
```
Deserializer deserializer(serialized_char_array);
deserializer.deserialize_size_t();
deserializer.deserialize_bool();
int int_value = deserializer.deserialize_int();
```
## Serialize Examples

`serialize_bool(bool bool_value)` (example: bool_value = true):
```
|  1|
```

`serialize_int(int int_value)` (example: int_value = -23):
```
|-23| -1| -1 | -1| 
```

`serialize_float(float float_value)` (example: float_value = 12.34): 
```
|-92|112| 69| 65|
```

`serialize_size_t(size_t size_t_value)` (example: size_t_value = 13):
```
| 13|  0|  0|  0|  0|  0|  0|  0|
```

`serialize_object(Object* object)` examples:  

Serialize String (example: "hello"):  
```
| 22|  0|  0|  0|  0|  0|  0|  0| - Serial size (size_t)
|  5|  0|  0|  0|  0|  0|  0|  0| - Length, excluding null terminator (size_t)
|104|101|108|108|111|  0|         - Char array
```

Serialize Put message (example: "172.13.2.0", "10.221.22.2", "hello"):  
```
|101|  0|  0|  0|  0|  0|  0|  0|                 - Serial size (size_t)

|  2|  0|  0|  0|  0|  0|  0|  0|                 - MsgKind enum (size_t)

| 27|  0|  0|  0|  0|  0|  0|  0|                 - Sender serial length (size_t)
| 10|  0|  0|  0|  0|  0|  0|  0|                 - Sender length (size_t)
| 49| 55| 50| 46| 49| 51| 46| 50| 46| 48|  0|     - Sender char array

| 28|  0|  0|  0|  0|  0|  0|  0|                 - Target serial length (size_t)
| 11|  0|  0|  0|  0|  0|  0|  0|                 - Target length (size_t)
| 49| 48| 46| 50| 50| 49| 46| 50| 50| 46| 50|  0| - Target char array

|  0|  0|  0|  0|  0|  0|  0|  0|                 - ID of the Message

| 22|  0|  0|  0|  0|  0|  0|  0|                 - Message serial size (size_t)
|  5|  0|  0|  0|  0|  0|  0|  0|                 - Message length (size_t)
|104|101|108|108|111|  0|                         - Message char array
```
