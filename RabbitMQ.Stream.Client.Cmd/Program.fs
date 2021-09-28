// Learn more about F# at http://docs.microsoft.com/dotnet/fsharp

open System
open System.Buffers
open System.Net
open RabbitMQ.Stream
open RabbitMQ.Stream.Client

// Define a function to construct a message to print
type Config =
    { UserName: string }

[<EntryPoint>]
let main argv =
    let mutable run = true
    let mutable publishingId = 0UL
    let mutable lastPublishingId = 0UL
    let mutable confirmed = 0
    task {
        let config = StreamSystemConfig(UserName = "guest",
                                        Password = "guest")
        let! system = StreamSystem.Create config
        let producerConfig = ProducerConfig(Stream = "s1",
                                            Reference = Guid.NewGuid().ToString(),
                                            MaxInFlight = 10000,
                                            ConfirmHandler = fun c -> confirmed <- confirmed + 1)
        let! producer = system.CreateProducer producerConfig
        let msg = Message "asdf"B
        while run do
            let! _ = producer.Send(publishingId, msg)
            publishingId <- publishingId + 1UL
            ()
    } |> Async.AwaitTask |> Async.Start
    async{
        while run do
            do! Async.Sleep 1000
            let p = publishingId;
            printfn "published %i msg/s" (p - lastPublishingId)
            lastPublishingId <- p
    } |> Async.Start
    //t.Wait()
    //printfn "Hello world %s" message
    Console.ReadKey()  |> ignore
    run <- false
    0 // return an integer exit code