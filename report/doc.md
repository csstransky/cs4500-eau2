# eau2
By Kaylin Devchand and Cristian Stransky

## Introduction
The eau2 system has three layers: the networking layer that contains the KVStore, the DataFrame layer, and the application layer. This system runs on each Node. There is also a rendezvous server that acts as the lead node which each Node registers to.

## Architecture
The networking layer is responsible for the KVStore and the communication between KVStores on other nodes. The KVStore is a hash map from a Key to a Value. The Key contains a name and a node index. The Value is a serialized blob of data (stored as a Serializer Object). Nodes send and receive Values from each other using Get, Value, and Put Messages. The KVStore inherits from the Node class, so that communication between the network and the kv store can be efficiently done.

The DataFrame is still represented as a list of columns. The columns of the dataframe, which previously used an array of fixed arrays, will now use an array of Keys which point to Values that are distributed arrays; this allows for different chunks of data to be distributed onto other Nodes' KVStores.  Each column contains a reference to the local KVStore as an entry point to the KVStore network. On top of the KVStore, is a KDStore (Key Dataframe Store). A KDStore contains a single field, KVStore, and abstracts out the code needed to put and get dataframes from the KVStore. 

The application can create a Dataframe through reading SoR files or generating elements for the Dataframe. The application contains a reference to the KDStore and Dataframes can be added to it. Dataframes can also be retrieved from the KDStore.

## Implementation

### Networking Layer
The implementation of the networking layer consists the following classes: Serialize, Message, Server, RServer, Node, Key, Value, and KVStore. The networking layer allows nodes to pass serialized data to each other.

Nodes can only communicate through serialized Messages. For each type of message, a sub class of Message is created that contains fields of data to send. The Message is then serialized through the Serialized class and passed to the node through a socket. The Message is deserialized through the Message::deserialize static method. The types of messages are Register, Directory, Kill, Complete, Get, GetAndWait, Value, and Put.

The system has a lead node represented by the class RServer (rendezvous server) and multiple other nodes represented by the class Node. Both of these classes inherit from the Server class to abstract out similar functionality. The Server monitors a socket that accepts incoming connections and creates new client sockets for each connection. The client sockets are also monitored for new messages and disconnections. The Server class contains the functionality for sending messages. Every time a new message is received the decode_message_ method is called to react to the message. The subclasses of Server have to implement this method and define the functionality. On creation, the Server creates a separate thread to monitor the network so that the application thread does not block network activity. On shutdown, the application thread and network thread are joined.

The RServer keeps track of all nodes in the network. When a Node comes online, it connects to the RServer and registers itself with its IP and node id. The RServer will then send a new complete list of nodes to all connected nodes with a Directory Message. The RServer will send a Kill message to all its clients when it gets a Complete message from every Node. This Complete message signifies that the Node has completed its application code. The Node connects to the RServer on startup and sends a Register Message to it. It will update its local list of nodes every time it receives a Directory Message.

The KVStore class is a subclass of Node. It contains a mapping of Keys to Values that represents its local store. KVStore overrides the decode_message_ method of Node to react to Get, WaitAndGet, Value, and Put Messages. All of the networking code is hidden in the Node class. The KVStore is a Node subclass so that it can easily send messages and response to other Node messages. The KVStore contains public methods to put and get Values in the store. When a get operation is performed, the node index of the key is checked. If the node index corresponds to the local KVStore, the value is fetched from the local KVStore. If the Value is determined to be on a different Node, a Get Message is created and sent to that Node and a Value Message is received from that Node. When a put operation is performed and the node index is the local KVStore, the key value pair is added to the map. Otherwise a Put Message is sent to the designated Node. When a Put Message is received, the KVStore places the key value pair in the Message in its map. When a Get Message is received, the KVStore looks up the key and sends a Value Message with the Value back to the Node.

The KVStore class stores its data in an String to Object map. It has a put method that takes in an object, serializes the object, then puts it into the map. There are five get methods. One get methods just returns the serialized object. The other four convert the serialized object into the four different array types (int, float, bool, String). The KVStore also stores its local node index and that can be returned with a getter.

The KVStore class also has a get queue, which is a String to IntArray map. This data structure is used to queue up wait and get requests. The get queue maps the key name to an array of socket descriptors waiting for the Value. Every time a put operation occurs, the get queue is checked for the key and the value is distributed to the nodes that requested it. Wait and get operations follow the same steps as the get operations except in the fact that it interacts with the get queue.

### Dataframe Layer
The DataFrame layer consists of the DataFrame class and supporting classes. The implementation of the Dataframe class is the same as previous assignments with row and column names stripped. The column classes contain an additional field that points to the KVStore. The data in the Column classes is represented as distributed arrays. The column classes contain a list of keys where each key maps to a Value blob of 1000 elements. The Value blobs can be on any KVStore in the network. To retrieve data, the get method of the KVStore is called with the key. Only full 1000 blocks are distributed. If a block contains less than 100 elements it is kept as a local array in the Column. 

The Column classes also have a buffered elements array. This is the last blob of elements and is used when constructing a Column so that every time an element is added, KVStore put and get do not need to be called. Columns create keys for their blobs by concating the dataframe name, column index in the dataframe, and blob index. Example:
```
ELEMENT_ARRAY_SIZE = 10;

IntColumn
-----
type_ = 'I'
kv_ = kv_store1
dataframe_name_ = "Main"
column_index_ = 12
size_ = 34
keys_ = [Key("Main_12_0", 2), Key("Main_12_1", 2), Key("Main_12_2", 2)]
buffered_elements_ = [31, 32, 33, 34]

kv_store1
-----
local_node_index_ = 1
kv_map_ = {
    // The actual IntColumn class is serialized in here with the DataFrame
    "Main" : Serializer('DataFrame Object')
}

kv_store2
-----
local_node_index_ = 2
kv_map_ = {
    // The contents of IntColumn are serialized elsewhere
    "Main_12_0" : Serializer([ 1,  2,  3,  4,  5,  6,  7,  8,  9, 10])
    "Main_12_1" : Serializer([11, 12, 13, 14, 15, 16, 17, 18, 19, 20])
    "Main_12_2" : Serializer([21, 22, 23, 24, 25, 26, 27, 28, 29, 30])
}
```

A network call is (mostly) avoided for every push_back(...), and sometimes a network call can be avoided for a get(...). The reason `buffered_elements_` is used is to allow the creation of a DataFrame to be quicker, and allow for only **3 network calls** to store the above IntColumn (which we assume is placed in a different KV_Store than the DataFrame for this example), instead of needing to make **34 network calls** for every element. 

This also has the added benefit of allowing a Row to simply use a ColumnArray for its fields, where each Column inside that ColumnArray **will not** have to make ANY network calls to set and put data into the DataFrame (especially with add_row(...), which is used often for our SoR file adapter).


### Application Layer
The Application class contains the KDStore of the node as a field. It also has the ability to retrieve the node id from the KDStore. The Application also contains a run method to run the application. This method is not implemented in Application and is to be implemented by subclasses of Application. To create a specific Application, a subclass of Application is created and the run method and helper methods are implemented. On deconstruction of the Application, the shutdown server method is called. That is a blocking call that waits for the kill message from the RServer before joining the networking thread and application thread and closing everything down. The same application can run on different nodes or different applications can be created. No application runs on the RServer.

## Use cases
The use case below creates a dataframe from a SoR file.

```
class CreateDataframe : public Application {
    public:
        char* filename_;
        Key k("sor", 0);
        
        CreateDataframe(size_t idx, char* filename): Application(idx) {
            filename_ = filename;
        }
        void run_() override {
            DataFrame* df = DataFrame::from_file(filename_);
            ...
            delete df;
        }
    }
}
```

## Open questions
+ If we were to use get() on a kv_store that doesn't have the kv pair, is it okay if we can error out?   
We expect that if you use get(), you know the key is already in the kv_store (wait_and_get should be used if you don't know if the key is there).
+ Are we going to need is_missing (from the refactor video) in our dataframe?

## Status
All of our code valgrinds. The application runs on multiple nodes using a complete network layer. The pseudo network approach was not taken. The demo class example given in the assignment description for Milestone 3 works. 

We are in the middle of refactor and deleting duplicate code but have not merged this refactor with our master branch. We have completed refactoring array to use a union for its payload instead of all the duplicate code. We have not implemented the array changes in the rest of the code yet so we are not submitting the refactored array. For the next milestone, we will have the refactored array changes reflected throughout our code, change our implementation of column to use one class, and delete unused code in map and dataframe. Serialization and deserialization is also being redesigned to reduce code. We can provide the link to our refactor branch to show that we are deleting the duplicate code (but right now the repo is private).

We also plan to add a cache to the kv store or column class to reduce network calls.

Steps to run our code:
1. builds the needed directory of executables  
``` 
make
```          
2. runs the trival application for Milestone 2  
```
make trivial
```
3. runs the demo application for Milestone 3
```
make demo
```
4. runs the tests for all of our code  
```
make test
```
5. runs a valgrind check on all our code (may take a while for `test_application`)
```
make valgrind
```
6. removes the directory with executables  
```
make clean
```