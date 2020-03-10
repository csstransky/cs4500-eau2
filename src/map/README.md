# Data Structure We Chose To Implement: Map

API: https://github.com/rohitppathak/-softdev-2020-ai-part2.git

## map.h contains the classes Map, SOMap, and SSMap.

### Map:
Map is the general Map class which inherits from Object, which means it overrides equals and hash. Its general purpose is to store a key-value pair, where in this class both the key and the value are Objects. The method put takes in a key and value and stores it in the Map. The method get takes in a key and returns the value for that key in the Map, null if there is no value for that key. There is other functionality of the Map for gettings its size, checking if it is empty, removing a value, checking if a key exists, checking if a value exists, adding all the key-value pairs of another Map, getting a List of all keys (unordered), and getting a List of all values (unordered).

### SOMap:
SOMap inherits from Map and stands for String-Object Map, which means that it stores its keys as type String and its values as type Object. This means that when a put operation is called, the method should return an error if the key passed in is not a String.

### SSMap:
SSMap inherits from SOMap and stands for String-String Map, which means that it stores both its keys and values as type String. This means that when a put operation is called, the method should return an error if the key passed in is not a String. Also, when the get operation is called, the return value should be able to be cast to a String.
