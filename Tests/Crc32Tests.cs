// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class FirstCrc32IsWrong : ICrc32
{
    private int _callCount = 0;

    public byte[] Hash(byte[] data)
    {
        _callCount++;
        return _callCount == 1
            ?
            // Return a wrong hash on the first call to simulate a CRC32 failure
            // second call will return the correct hash
            [0x00, 0x00, 0x00, 0x04]
            : System.IO.Hashing.Crc32.Hash(data);
    }

    public Func<IConsumer, ChunkAction> FailAction { get; set; }
}

public class Crc32Tests(ITestOutputHelper testOutputHelper)
{
    /// <summary>
    /// Tests the Crc32 functionality of the consumer.
    /// In this case, the first Crc32 is wrong, so the consumer should skip the chunk.
    /// So the consumer should receive the second chunk.
    /// </summary>
    [Fact]
    public async Task Crc32ShouldSkipChunk()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var completionSource = new TaskCompletionSource<MessageContext>();
        var consumerConfig = new ConsumerConfig(system, stream)
        {
            Crc32 = new FirstCrc32IsWrong() { FailAction = (_) => ChunkAction.Skip },
            InitialCredits = 1,
            MessageHandler = (_, _, messageContext, _) =>
            {
                completionSource.TrySetResult(messageContext);
                return Task.CompletedTask;
            }
        };
        var consumer = await Consumer.Create(consumerConfig);
        await SystemUtils.PublishMessages(system, stream, 3, "1", testOutputHelper);
        await SystemUtils.WaitAsync();
        await SystemUtils.PublishMessages(system, stream, 5, "2", testOutputHelper);
        var messageContext = await completionSource.Task;
        Assert.True(1 != messageContext.ChunkId);
        Assert.True(consumer.IsOpen());
        await consumer.Close();
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    /// <summary>
    /// Tests the Crc32 functionality of the consumer.
    /// Simulate a CRC32 failure on the first chunk,
    /// and add a custom action to close the consumer.
    /// the consumer should be closed and the chunk should be skipped.
    /// </summary>
    [Fact]
    public async Task Crc32ShouldCloseTheConsumer()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var consumerConfig = new ConsumerConfig(system, stream)
        {
            Crc32 = new FirstCrc32IsWrong()
            {
                FailAction = consumer =>
                {
                    consumer.Close();
                    return ChunkAction.Skip; // Skip the chunk and close the consumer
                }
            },
            MessageHandler = (_, _, _, _) => Task.CompletedTask
        };

        var consumer = await Consumer.Create(consumerConfig);
        await SystemUtils.PublishMessages(system, stream, 3, "1", testOutputHelper);
        await SystemUtils.WaitAsync(TimeSpan.FromMilliseconds(500));

        Assert.False(consumer.IsOpen());
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }

    /// <summary>
    /// Here we test an edge case where  Crc32 is wrong,
    /// but the consumer should still process the chunk.
    /// by given the FailAction as TryToProcess.
    /// In real life when a a CRC32 is wrong the consumer can't process the chunk,
    /// this is to give more flexibility to the user.
    /// </summary>
    [Fact]
    public async Task Crc32ShouldParseTheChunk()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var completionSource = new TaskCompletionSource<MessageContext>();
        var consumerConfig = new ConsumerConfig(system, stream)
        {
            Crc32 = new FirstCrc32IsWrong() { FailAction = (_) => ChunkAction.TryToProcess },
            MessageHandler = (_, _, messageContext, _) =>
            {
                completionSource.TrySetResult(messageContext);
                return Task.CompletedTask;
            }
        };
        var consumer = await Consumer.Create(consumerConfig);
        await SystemUtils.PublishMessages(system, stream, 3, "1", testOutputHelper);
        await SystemUtils.WaitAsync();
        var messageContext = await completionSource.Task;
        Assert.True(messageContext.Offset == 0);
        Assert.True(0 == messageContext.ChunkId);
        Assert.True(3 == messageContext.ChunkMessagesCount);
        Assert.True(consumer.IsOpen());
        await consumer.Close();
        await SystemUtils.CleanUpStreamSystem(system, stream);
    }
}
