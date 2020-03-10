# eau2
By Kaylin Devchand and Cristian Stransky

## Introduction:
The eau2 system has three layers: the networking layer that contains the KVStore, the Dataframe layer, and the application layer. This systems runs on each Node. There is also a rendevzous server that acts as the lead node which each Node registers to. 

## Architecture:

The networking layer is reponsible for the KVStore and the communication between KVStores on other nodes. The KVStore is a hash map from a Key to a Value. The Key contains a name and a node index. The Value is a serialized blob of data. Nodes send and receive Values from each other using Get, Value, and Put Messages.

The Dataframe is still represented as a list of columns. The columns of the dataframe are implemented as distrubuted arrays where different chunks of data can reside on other Nodes' KVStores. Each column contains a reference to the local KVStore as an entry point to the KVStore network. 

The application can create a Dataframe through reading SoR files or generating elements for the Dataframe. The application contains a reference to the local KVStore and Dataframes can be added to it. Dataframes can also be retrieved from the KVStore.

## Implementation:
When a get operation is performed, the node index of the key is checked. If the node index corresponds to the local KVStore, the value is fetched from the local KVStore. If the Value is determined to be on a different Node, a Get Message is created and sent to that Node and a Value Message is received from that Node.

## Use cases:

## Open questions:

## Status: