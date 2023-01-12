// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;

namespace Tests;

using Xunit;

public class FromToAmqpTests
{
    // private readonly ITestOutputHelper _testOutputHelper;
    //
    // public FromToAmqpTests(ITestOutputHelper testOutputHelper)
    // {
    //     _testOutputHelper = testOutputHelper;
    // }

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
        channel.QueueDelete(stream);
        channel.Close();
    }
}
