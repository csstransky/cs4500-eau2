# Array

API: https://github.com/chasebish/cs4500_assignment1_part2.git

### Object:
Overview:
- A basic CwC class that is supposed to sit at the top of the object heirarchy

Contents:
- basic constructor and destructor
- virtual methods to be overriden
- hashing method to assist with equality

### String:
Overview:
- Subclass of Object
- A String class that is an immutable representation of char[]

Contents:
- basic constructors and destructors
- overriden methods from Object
- methods specific to String including concat and length

### Array:
Overview:
- Subclass of Object
- 5 different implementations, BoolArray, FloatArray, IntArray, ObjectArray, StringArray (see [Piazza](https://piazza.com/class/k51bluky59n2jr?cid=331))

Contents:
- basic constructors and destructors
- overriden methods from Object
- methods specific to Array including clear, concat, get, index_of, length, pop, push, remove, replace