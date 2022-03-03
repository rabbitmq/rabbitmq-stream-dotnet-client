Sub Entry compress codecs
---

By default the client implements only `None` and `Gzip` see [here](https://github.com/rabbitmq/rabbitmq-stream-dotnet-client#sub-entries-batching) for more details.


You need to implement `ICompressionCodec` interface to create a new codec. 
Here you can find all the missing implementations:
 - [lz4](./lz4) 
 - Snappy not implemented yet
 - Zstd not implemented yet
