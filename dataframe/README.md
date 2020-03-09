# DataFrame

A DataFrame is simply an object that holds a Schema and an array of Columns. 
NOTE: The array of Columns will double in size when a Column is added that will surpass its array length. 

A DataFrame is mainly an abstraction that holds Columns, with even its properties being handled by the Schema. Most of DataFrame is using the functions with these classes.

The interesting thing to our DataFrame is how we handle adding a new Column of a different row size. Also, all columns added are COPIED into the DataFrame (this mainly let's us increase the sizes of Columns inside the DataFrame as we like).

What happens when a smaller column is added:
```
DF:
|1|2|
|3|4|
|4|6|
|7|8|
Column being added:
|9|
|9|
DF after adding:
|1|2|9|
|3|4|9|
|4|6|0|
|7|8|0|
```
What happens when a larger column is added:
```
DF:
|1|2|
|3|4|
|4|6|
Column being added:
|9|
|9|
|9|
|9|
|9|
|9|
DF after adding:
|1|2|9|
|3|4|9|
|4|6|9|
|0|0|9|
|0|0|9|
|0|0|9|
```
NOTE: Default values that are added are noted at the top of dataframe.h:
```
const int DEFAULT_INT_VALUE = 0;
const float DEFAULT_FLOAT_VALUE = 0;
const bool DEFAULT_BOOL_VALUE = 0;
const String* DEFAULT_STRING_VALUE = nullptr;
```
# Column
Columns are split into primitive types (such as IntColumn, FloatColumn, etc) that hold their respective array of values.

In order to make sure that the payload of a column is never copied, a paging system using an array of arrays is used.  
Example:
```
IntColumn:
// Has an int array which holds chunks of 6
[3|7| | ]
|2|9|
|1| |
|3| |
|2| |
|4| |
// Trying to access elements will work exactly the same, 
// math is handled in background of the function
IntColumn->get(2) = 1
IntColumn->get(7) = 9
```
NOTE: The default value of an array chunk is determined at the top of dataframe.h (we thought that 100 was a decent size, this of course can be adjusted):
```
const int ELEMENT_ARRAY_SIZE = 100;
```
NOTE: When there are enough elements added to the Column that needs the array holding the chunks to increase, it will double its size.
# Schema
A Schema will hold the structure of how a DataFrame and Row should be made. A Schema holds a character array called "types_" that will foresee the structure of the DataFrame or Row.  
Example:
```
Schema("IIFBS")
Row 1 = IntColumn
Row 2 = IntColumn
Row 3 = FloatColumn
Row 4 = BoolColumn
Row 5 = StringColumn
```
Schema will also keep track of the number of Rows and Columns, so whenever a DataFrame for example adds a Row, it will also need to update its Schema.

NOTE: Failure to adhere to the schema, or use different letters than "IFBS" WILL cause errors to be thrown.

There is also a basic String array of Column and Row names, that will double its size when capacity is reached. The interesting thing is that neither a DataFrame or Row have a way to change or assign a Row's name, so every single name for every Row in a schema will be nullptr.

# Rower & Fielder

Rower and Fielder are both general parent classes that are used for functions like map(...) and filter(...) on a DataFrame.
+ Rower uses its accept function on every Row on the DataFrame
+ Fielder uses its accept function on every field on a Row
+ The two are often used in conjuction to traverse an entire DataFrame (Rower sometimes has a Fielder field)

NOTE: In order to properly use Rower and Fielder, there must be subclasses made from the parent class to be used on the DataFrame or Row.

```
DataFrame df = new DataFrame("II");
PrintRower pr = new PrintPower();
// The Rower will now apply its printing accept function to every Row
df->map(pr);
// The Rower uses its PrinterFielder inside to traverse every Cell
```

# Examples of DataFrame
Basic functionality of adding Rows and Columns to a DataFrame:
```
Schema s("IIII");
DataFrame* df = new DataFrame(s);
// Imagine we fill the df with a lot of int values
|1|3|4|5|
|3|3|3|3|
|1|1|1|1|

// Now we add a column
IntColumn col = new IntColumn(2, 4, 8, 1);
df.add_column(col);

// The resulting dataframe is:
|1|3|4|5|4|
|3|3|3|3|8|
|1|1|1|1|1|

// Now we add a row
Row* r = new Row(s);
r->set(0, 3);
r->set(1, 2);
r->set(2, 1);
r->set(3, 7);
r->set(4, 9);
df.add_row(*r);

// The resulting dataframe is:
|1|3|4|5|4|
|3|3|3|3|8|
|1|1|1|1|1|
|3|2|1|7|9|

```

Have a DataFrame of Ints that has all of its values added to a Column:
```
Schema s("IIII");
DataFrame* df = new DataFrame(s);
// Imagine we fill the df with a lot of int values
|1|3|4|5|
|3|3|3|3|
|1|1|1|1|

IntColumn sum_results = new IntColumn();
df->add_column(sum_results, nullptr);
// Now DataFrame looks like this:
|1|3|4|5|0|
|3|3|3|3|0|
|1|1|1|1|0|

// Creates a Rower that will sum all the elements of a Row and add it to 
// the last Column
AddRower add_rower = new AddRower(&df);
// This will edit the DataFrame
df->map(add_rower);
// DataFrame now looks like this:
|1|3|4|5|13|
|3|3|3|3|12|
|1|1|1|1|4|
```
Getting a filtered DataFrame with Rows that are non empty:
```
Schema s("FBIS");
DataFrame* df = new DataFrame(S);
// Imagine we fill the df with a lot of different values
|12.2|false|   12| "hi"|
|0   |false|    0|nlptr|
|0   |false|    0|nlptr|
|0   | true|    0|"bye"|
|14.1|false|    3|nlptr|
|0   |false|    0|nlptr|
|0   | true|    2|nlptr|
|3.2 |     |    0|"lol"|
|0   |false|    0|nlptr|

// We create a Rower that will return true on its accept function
// for Rows with at least 1 filled in field
Rower* filter_rower = new NonEmptyFilterRower();
// A brand new dataframe is made with its own values of non empty elements
DataFrame* filtered_dataframe = df.filter(*filter_rower);
// The filtered_dataframe will look like this:
|12.2|false|   12| "hi"|
|0   | true|    0|"bye"|
|14.1|false|    3|nlptr|
|0   | true|    2|nlptr|
|3.2 |     |    0|"lol"|
```