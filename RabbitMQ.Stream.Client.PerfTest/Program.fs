// Learn more about F# at http://docs.microsoft.com/dotnet/fsharp

open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open RabbitMQ.Stream.Client

let formatCount (n: int) = n.ToString("N0")

let formatRate (delta: int) (elapsed: TimeSpan) =
    let rate = if elapsed.TotalSeconds > 0.0 then float delta / elapsed.TotalSeconds else 0.0
    rate.ToString("N0")

let formatThroughput (bytesPerSecond: float) =
    let units = [| "B/s"; "KB/s"; "MB/s"; "GB/s" |]
    let mutable value = bytesPerSecond
    let mutable unitIndex = 0
    while value >= 1024.0 && unitIndex < units.Length - 1 do
        value <- value / 1024.0
        unitIndex <- unitIndex + 1
    sprintf "%.2f %s" value units.[unitIndex]

// Define a function to construct a message to print
[<EntryPoint>]
let main argv =
    ThreadPool.SetMinThreads(16 * Environment.ProcessorCount, 16 * Environment.ProcessorCount) |> ignore
    let mutable run = true
    let mutable publishingId = 0UL
    let mutable lastPublished = 0
    let mutable lastFrames = 0
    let mutable lastConfirmed = 0
    let mutable lastConsumed = 0
    let mutable consumed = 0
    let mutable confirmed = 0
    let mutable prod = null
    let streamName = "dotnet-perftest"
    let payload = "asdf"B
    let consumerConfig = RawConsumerConfig(streamName,
                                        Reference = Guid.NewGuid().ToString(),
                                        MessageHandler =
                                            fun c ctx m ->
                                                consumed <- consumed + 1
                                                Task.CompletedTask )
    let t = task {
        let config = StreamSystemConfig(UserName = "guest", Password = "guest")
        let! system = StreamSystem.Create config
        let! stream = system.CreateStream(StreamSpec(streamName))
        printfn $"Stream: {streamName}"
        let! consumer = system.CreateRawConsumer(consumerConfig)
        let producerConfig = RawProducerConfig(streamName,
                                            Reference = null,
                                            MaxInFlight = 10000,
                                            MessagesBufferSize = 10000,
                                            ConfirmHandler = fun c -> confirmed <- confirmed + 1)
        let! producer = system.CreateRawProducer producerConfig
        //make producer available to metrics async
        prod <- producer
        let msg = Message payload
        while run do
            let! _ = producer.Send(publishingId, msg)
            publishingId <- publishingId + 1UL
            ()
    }

    let stopwatch = Stopwatch.StartNew()
    async {
        while run do
            let intervalStart = stopwatch.Elapsed
            do! Async.Sleep 1000
            let elapsed = stopwatch.Elapsed - intervalStart

            let published = prod.MessagesSent
            let frames = prod.PublishCommandsSent
            let confirmedNow = confirmed
            let consumedNow = consumed

            let publishedDelta = published - lastPublished
            let throughput = formatThroughput (float publishedDelta * float payload.Length / elapsed.TotalSeconds)

            printfn $"published %s{formatRate publishedDelta elapsed} msg/s (%s{formatRate (frames - lastFrames) elapsed} frames/s, {throughput}) | confirmed %s{formatRate (confirmedNow - lastConfirmed) elapsed} msg/s | consumed %s{formatRate (consumedNow - lastConsumed) elapsed} msg/s | totals: confirm frames %s{formatCount prod.ConfirmFrames}, incoming frames %s{formatCount prod.IncomingFrames}, pending %s{formatCount prod.PendingCount}"

            lastPublished <- published
            lastFrames <- frames
            lastConfirmed <- confirmedNow
            lastConsumed <- consumedNow
    } |> Async.Start
    Console.ReadKey() |> ignore
    run <- false
    0
