// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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

    [Fact]
    public void TestFromToAmqp()
    {
        var factory = new ConnectionFactory();
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.QueueDeclare("test", false, false, false, null);
        var properties = channel.CreateBasicProperties();
        properties.Persistent = true;
        properties.Headers = new Dictionary<string, object>
        {
            {"foo", "bar"}
        };
        channel.BasicPublish("", "test", properties, Encoding.UTF8.GetBytes("Hello World!"));
        var consumer = new EventingBasicConsumer(channel);
        var tcs = new TaskCompletionSource<BasicDeliverEventArgs>();
        consumer.Received += (sender, args) => tcs.SetResult(args);
        channel.BasicConsume("test", true, consumer);
        var result = tcs.Task.Result;
        Assert.Equal("bar", System.Text.Encoding.UTF8.GetString(result.BasicProperties.Headers["foo"] as byte[] ?? Array.Empty<byte>()));
    }
}
