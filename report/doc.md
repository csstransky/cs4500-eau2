# eau2
By Kaylin Devchand and Cristian Stransky

## Introduction
The eau2 system has three layers: the networking layer that contains the KVStore, the DataFrame layer, and the application layer. This system runs on each Node. There is also a rendezvous server that acts as the lead node which each Node registers to.

## Architecture
The networking layer is responsible for the KVStore and the communication between KVStores on other nodes. The KVStore is a hash map from a Key to a Value. The Key contains a name and a node index. The Value is a serialized blob of data (stored as a Serializer Object). Nodes send and receive Values from each other using Get, Value, and Put Messages.

The DataFrame is still represented as a list of columns. The columns of the dataframe, which previously used an array of fixed arrays, will now use an array of Keys which point to Values that are distributed arrays; this allows for different chunks of data to be distributed onto other Nodes' KVStores.  Each column contains a reference to the local KVStore as an entry point to the KVStore network.

The application can create a Dataframe through reading SoR files or generating elements for the Dataframe. The application contains a reference to the local KVStore and Dataframes can be added to it. Dataframes can also be retrieved from the KVStore.

## Implementation

### Networking Layer
The implementation of the networking layer consists the following classes: Serialize, Message, Server, RServer, Node, Key, Value, and KVStore. The networking layer allows nodes to pass serialized data to each other.

Nodes can only communicate through serialized Messages. For each type of message, a sub class of Message is created that contains fields of data to send. The Message is then serialized through the Serialized class and passed to the node through a socket. The Message is deserialized through the Message::deserialize static method. The types of messages are Register, Directory, Kill, Get, Value, and Put.

The system has a lead node represented by the class RServer (rendezvous server) and multiple other nodes represented by the class Node. Both of these classes inherit from the Server class to abstract out similar functionality. The Server monitors a socket that accepts incoming connections and creates new client sockets for each connection. The client sockets are also monitored for new messages and disconnections. The Server class contains the functionality for sending messages. Every time a new message is received the decode_message_ method is called to react to the message. The subclasses of Server have to implement this method and define the functionality.

The RServer keeps track of all nodes in the network. When a Node comes online, it connects to the RServer and registers itself with its IP and node id. The RServer will then send a new complete list of nodes to all connected nodes with a Directory Message. The RServer will send a Kill message to all its clients when a timeout has been reached and it is ready to shutdown. The Node connects to the RServer on startup and sends a Register Message to it. It will update its local list of nodes every time it receives a Directory Message.

The KVStore class is a subclass of Node. It contains a mapping of Keys to Values that represents its local store. KVStore overrides the decode_message_ method of Node to react to Get, Value, and Put Messages. All of the networking code is hidden in the Node class. The KVStore is a Node subclass so that it can easily send messages and response to other Node messages. The KVStore contains public methods to put and get Values in the store. When a get operation is performed, the node index of the key is checked. If the node index corresponds to the local KVStore, the value is fetched from the local KVStore. If the Value is determined to be on a different Node, a Get Message is created and sent to that Node and a Value Message is received from that Node. The get method can be blocking or non blocking. When a put operation is performed and the node index is the local KVStore, the key value pair is added to the map. Otherwise a Put Message is sent to the designated Node. When a Put Message is received, the KVStore places the key value pair in the Message in its map. When a Get Message is received, the KVStore looks up the key and sends a Value Message with the Value back to the Node.

### Dataframe Layer
The DataFrame layer consists of the DataFrame class and supporting classes. The implementation of the Dataframe class is the same as previous assignments with row and column names stripped. The column classes contain an additional field that points to the KVStore. The data in the Column classes is represented as distributed arrays. The column classes contain a list of keys where each key maps to a Value blob of 100 elements. The Value blobs can be on any KVStore in the network. To retrieve data, the get method of the KVStore is called with the key. Only full 100 blocks are distributed. If a block contains less than 100 elements it is kept as a local array in the Column.

### Application Layer
The Application class contains the KVStore of the node as a field. It also has the ability to retrieve the node id from the KVStore. The Application also contains a run method to run the application. This method is not implemented in Application and is to be implemented by subclasses of Application. To create a specific Application, a subclass of Application is created and the run method and helper methods are implemented. The same application can run on different nodes or different applications can be created. No application runs on the RServer.

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
            SoR* sor = new SoR(filename_);
            DataFrame* df = sor->get_dataframe();
            kv.put(k, df);
        }
    }
}
```

## Open questions
+ What is exactly is the class structure of this entire assignment? Example:   
Node  
\- Application* app  
\- KV_Store* k  
\- size_t index  
\- String** node_ips 
+ Should we have a "lead" KV store that knows ALL of the keys?    
Will that be the job of the Rendezvous Server instead?
+ Who will initialize keys?    
Does our Node create generated Keys with predictable names, or are they passed in through a constructor or push method?  
+ Should a DataFrame split its data into different chunks across Nodes (all done in the background), or should we instead make many Keys of smaller DataFrames to split our read-in data?   
Will our DataFrame functionality stay the same and keep constant "integrity" (all on 1 node)?  
+ (Somewhat answered on Piazza) Can we use the C++ version of Map? 
+ What is the average size of the SoR file we will need to stores? 100MB? 1GB? 100GB?

## Status
Most of the old code has been migrated (all code that will be used), tests have been written for all of this code, and everything valgrinds nicely.
 
The next step will be to actually write the code for Application, Nodes, Keys, and knowing when to properly distribute data amongst nodes with networking implemented. Progress can start now, but some clarification on how the KV store will be optimized to hold DataFrames will allow for the project to start with confidence.
