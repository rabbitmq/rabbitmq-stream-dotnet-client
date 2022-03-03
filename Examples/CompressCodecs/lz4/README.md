Lz4 Compress codec
---

You need to add https://github.com/MiloszKrajewski/K4os.Compression.LZ4 as dependency


How to use:

- Register the codec
```csharp
 StreamCompressionCodecs.RegisterCodec<StreamLz4Codec>(CompressionType.Lz4);
```

- Send messages:
```csharp
var messages = new List<Message>();
...
 await producerLog.Send(1, messages, CompressionType.Lz4);               
```