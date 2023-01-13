// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;
using Xunit.Abstractions;

namespace Tests;

using Xunit;

public class FromToAmqpTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    public FromToAmqpTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    /// <summary>
    /// This test is to ensure that the conversion from AMQP to Stream AMQP 1.0 is correct.
    /// Stream sends the message and AMQP client reads it.
    /// In this case the server decodes anc converts the message
    /// </summary>
    [Fact]
    public async void Amqp091ShouldReadTheAmqp10Properties()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var producer = await Producer.Create(new ProducerConfig(system, stream));
        await producer.Send(new Message(Encoding.ASCII.GetBytes("FromStream"))
        {
            Properties = new Properties()
            {
                MessageId = "年 6 月",
                CorrelationId = "10000_00000",
                ContentType = "text/plain",
                ContentEncoding = "utf-8",
                UserId = Encoding.ASCII.GetBytes("MY_USER_ID"),
                GroupSequence = 601,
                ReplyToGroupId = "ReplyToGroupId",
                GroupId = "GroupId",
            },
            ApplicationProperties = new ApplicationProperties() { { "stream_key", "stream_value" } }
        });

        var factory = new ConnectionFactory();
        using var connection = factory.CreateConnection();
        var channel = connection.CreateModel();
        var consumer = new EventingBasicConsumer(channel);
        var tcs = new TaskCompletionSource<BasicDeliverEventArgs>();
        consumer.Received += (sender, ea) =>
        {
            tcs.SetResult(ea);
            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
        };
        channel.BasicQos(0, 100, false);
        channel.BasicConsume(stream, false, "consumerTag",
            arguments: new Dictionary<string, object>() { { "x-stream-offset", "first" } }, consumer);
        var result = tcs.Task.Result;
        Assert.Equal("FromStream", Encoding.ASCII.GetString(result.Body.ToArray()));
        Assert.Equal("年 6 月", result.BasicProperties.MessageId);
        Assert.Equal("10000_00000", result.BasicProperties.CorrelationId);
        Assert.Equal("text/plain", result.BasicProperties.ContentType);
        Assert.Equal("utf-8", result.BasicProperties.ContentEncoding);
        Assert.Equal("MY_USER_ID", result.BasicProperties.UserId);
        Assert.Equal("stream_value",
            Encoding.ASCII.GetString(result.BasicProperties.Headers["stream_key"] as byte[] ?? Array.Empty<byte>()));
        channel.QueueDelete(stream);
        channel.Close();
    }

    /// <summary>
    /// This test is to ensure that the conversion from AMQP 091 to Stream AMQP 1.0 is correct.
    /// AMQP sends the message and Stream has to read it.
    /// In this case the server decodes anc converts the message
    /// </summary>
    [Fact]
    public async void Amqp10ShouldReadTheAmqp019Properties()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);

        var factory = new ConnectionFactory();
        using var connection = factory.CreateConnection();
        var channel = connection.CreateModel();
        var properties = channel.CreateBasicProperties();

        properties.MessageId = "年 6 月";
        properties.CorrelationId = "10000_00000";
        properties.ContentType = "text/plain";
        properties.ContentEncoding = "utf-8";
        properties.UserId = "guest";
        properties.Headers = new Dictionary<string, object>()
        {
            {"stream_key", "stream_value"},
            {"stream_key4", "Alan Mathison Turing（1912 年 6 月 23 日"},
        };
        channel.BasicPublish("", stream, properties, Encoding.ASCII.GetBytes("FromAMQP"));
        var tcs = new TaskCompletionSource<Message>();

        var consumer = await Consumer.Create(new ConsumerConfig(system, stream)
        {
            OffsetSpec = new OffsetTypeFirst(),
            MessageHandler = async (_, _, _, message) =>
            {
                tcs.SetResult(message);
                await Task.CompletedTask;
            }
        });

        new Utils<Message>(_testOutputHelper).WaitUntilTaskCompletes(tcs);
        var result = tcs.Task.Result;
        Assert.Equal("FromAMQP", Encoding.ASCII.GetString(result.Data.Contents.ToArray()));
        Assert.Equal("年 6 月", result.Properties.MessageId);
        Assert.Equal("10000_00000", result.Properties.CorrelationId);
        Assert.Equal("text/plain", result.Properties.ContentType);
        Assert.Equal("utf-8", result.Properties.ContentEncoding);
        Assert.Equal(Encoding.Default.GetBytes("guest"), result.Properties.UserId);
        Assert.Equal("stream_value", result.ApplicationProperties["stream_key"]);
        Assert.Equal("Alan Mathison Turing（1912 年 6 月 23 日", result.ApplicationProperties["stream_key4"]);
        await consumer.Close();
        await system.DeleteStream(stream);
        await system.Close();
    }

    /// <summary>
    /// The file message_from_version_1_0_0 was generated with the 1.0.0 version of the client
    /// due of this issue https://github.com/rabbitmq/rabbitmq-stream-dotnet-client/pull/211 the write is changed
    /// but the read must be compatible
    /// </summary> 
    [Fact]
    public void DecodeMessageFrom100Version()
    {
        var data = SystemUtils.GetFileContent("message_from_version_1_0_0");
        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(data));
        var msg = Message.From(ref reader, (uint)reader.Length);
        Assert.Equal("Message100", Encoding.ASCII.GetString(msg.Data.Contents.ToArray()));
        Assert.Equal("MyMessageId", msg.Properties.MessageId);
        Assert.Equal("MyCorrelationId", msg.Properties.CorrelationId);
        Assert.Equal("text/plain", msg.Properties.ContentType);
        Assert.Equal("utf-8", msg.Properties.ContentEncoding);
        Assert.Equal("guest", Encoding.UTF8.GetString(msg.Properties.UserId));
        Assert.Equal((uint)9999, msg.Properties.GroupSequence);
        Assert.Equal("MyReplyToGroupId", msg.Properties.ReplyToGroupId);
        Assert.Equal("value", msg.ApplicationProperties["key_string"]);
        Assert.Equal(1111, msg.ApplicationProperties["key2_int"]);
        Assert.Equal(10_000_000_000, msg.ApplicationProperties["key2_decimal"]);
        Assert.Equal(true, msg.ApplicationProperties["key2_bool"]);
    }

    [Fact]
    public async void Amqp091ShouldReadTheAmqp10Properties1000Messages()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        var producer = await Producer.Create(new ProducerConfig(system, stream));
        const int NumberOfMessages = 1000;
        for (var i = 0; i < NumberOfMessages; i++)
        {
            await producer.Send(new Message(Encoding.ASCII.GetBytes($"FromStream{i}"))
            {
                Properties = new Properties()
                {
                    MessageId = $"Alan Mathison Turing（1912 年 6 月 23 日 - 1954 年 6 月 7 日）是英国数学家、计算机科学家、逻辑学家、密码分析家、哲学家和理论生物学家。 [6] 图灵在理论计算机科学的发展中具有很大的影响力，用图灵机提供了算法和计算概念的形式化，可以被认为是通用计算机的模型。[7][8][9] 他被广泛认为是理论计算机科学和人工智能之父{i}",
                    CorrelationId = "10000_00000",
                    ContentType = "text/plain",
                    ContentEncoding = "utf-8",
                    UserId = Encoding.ASCII.GetBytes("MY_USER_ID"),
                    GroupSequence = 601,
                    ReplyToGroupId = "ReplyToGroupId",
                    GroupId = "GroupId",

                },
                ApplicationProperties = new ApplicationProperties()
                {
                    {"stream_key", "stream_value"},
                    {"stream_key2",  100},
                    {"stream_key3", 10_000_009},
                    {"stream_key4", "Alan Mathison Turing（1912 年 6 月 23 日"},
                }
            });
        }

        var factory = new ConnectionFactory();
        using var connection = factory.CreateConnection();
        var channel = connection.CreateModel();
        var consumer = new EventingBasicConsumer(channel);
        var consumed = 0;
        var tcs = new TaskCompletionSource<int>();
        consumer.Received += (sender, ea) =>
        {
            if (Interlocked.Increment(ref consumed) == NumberOfMessages)
            {
                tcs.SetResult(consumed);
            }

            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
        };
        channel.BasicQos(0, 100, false);
        channel.BasicConsume(stream, false, "consumerTag",
            arguments: new Dictionary<string, object>() { { "x-stream-offset", "first" } }, consumer);
        new Utils<int>(_testOutputHelper).WaitUntilTaskCompletes(tcs);
        Assert.Equal(NumberOfMessages, tcs.Task.Result);
        channel.QueueDelete(stream);
        channel.Close();
    }

    [Fact]
    public async void Amqp10ShouldReadTheAmqp019Properties1000Messages()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);

        var factory = new ConnectionFactory();
        using var connection = factory.CreateConnection();
        var channel = connection.CreateModel();
        var properties = channel.CreateBasicProperties();
        const int NumberOfMessages = 1000;
        for (var i = 0; i < NumberOfMessages; i++)
        {
            properties.MessageId = $"messageId{i}";
            properties.CorrelationId = "10000_00000";
            properties.ContentType = "text/plain";
            properties.ContentEncoding = "utf-8";
            properties.UserId = "guest";
            properties.Headers = new Dictionary<string, object>() { { "stream_key", "stream_value" } };
            channel.BasicPublish("", stream, properties, Encoding.ASCII.GetBytes($"FromAMQP{i}"));
        }

        var tcs = new TaskCompletionSource<int>();
        var consumed = 0;
        var consumer = await Consumer.Create(new ConsumerConfig(system, stream)
        {
            OffsetSpec = new OffsetTypeFirst(),
            MessageHandler = async (_, _, _, message) =>
            {
                if (Interlocked.Increment(ref consumed) == NumberOfMessages)
                {
                    tcs.SetResult(consumed);
                }

                await Task.CompletedTask;
            }
        });

        new Utils<int>(_testOutputHelper).WaitUntilTaskCompletes(tcs);
        var result = tcs.Task.Result;
        Assert.Equal(NumberOfMessages, result);
        await consumer.Close();
        await system.DeleteStream(stream);
        await system.Close();
    }
}
