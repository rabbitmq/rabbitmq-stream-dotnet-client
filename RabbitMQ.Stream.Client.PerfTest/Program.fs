// Learn more about F# at http://docs.microsoft.com/dotnet/fsharp

open System
open System.Buffers
open System.Net
open System.Threading
open System.Threading.Tasks
open RabbitMQ.Stream
open RabbitMQ.Stream.Client

// Define a function to construct a message to print
[<EntryPoint>]
let main argv =
    ThreadPool.SetMinThreads(16 * Environment.ProcessorCount, 16 * Environment.ProcessorCount) |> ignore
    let mutable run = true
    let mutable publishingId = 0UL
    let mutable lastPublishingId = 0
    let mutable lastConfirmed = 0
    let mutable consumed = 0
    let mutable confirmed = 0
    let mutable prod = null
    let streamName = "dotnet-perftest"
    let consumerConfig = RawConsumerConfig(streamName,
                                        Reference = Guid.NewGuid().ToString(),
                                        MessageHandler =
                                            fun c ctx m ->
                                                
                                                consumed <- consumed + 1
                                                Task.CompletedTask )
    let t = task {
        let config = StreamSystemConfig(UserName = "guest",
                                        Password = "guest")
        let! system = StreamSystem.Create config
        let! stream = system.CreateStream(StreamSpec(streamName))
        printfn $"Stream: {streamName}"
        let! consumer = system.CreateRawConsumer(consumerConfig)
        let producerConfig = RawProducerConfig(streamName,
                                            Reference = null,
                                            MaxInFlight = 10000,
                                            ConfirmHandler = fun c -> confirmed <- confirmed + 1)
        let! producer = system.CreateRawProducer producerConfig
        //make producer available to metrics async
        prod <- producer
        let msg = Message "asdf"B
        while run do
            let! _ = producer.Send(publishingId, msg)
            publishingId <- publishingId + 1UL
            ()
    }
    
    let mutable lastFrames = 0
    let mutable lastConsumed = 0
    async{
        while run do
            do! Async.Sleep 1000
            let p = prod.MessagesSent
            let f = prod.PublishCommandsSent
            let c = confirmed
            let cs = consumed;
            printfn $"published %i{p - lastPublishingId} msg/s in %i{f - lastFrames} publish frames, confirmed %i{c - lastConfirmed} msg/s, consumed: %i{c - lastConsumed} msg/sec total confirm frames %i{prod.ConfirmFrames} %i{prod.IncomingFrames} pending commands: {prod.PendingCount} "
            lastConsumed <- cs
            lastFrames <- f
            lastPublishingId <- p
            lastConfirmed <- c
    } |> Async.Start
    //t.Wait()
    Console.ReadKey()  |> ignore
    run <- false
    0