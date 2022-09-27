// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class SuperStreamTests
{
    private readonly ITestOutputHelper _testOutputHelper;

    public SuperStreamTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Serializable]
    public class MessageIdToStream
    {
        public string StreamExpected { get; set; }
        public string MessageId { get; set; }
    }

    private class MessageIdToStreamTestCases : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-02", MessageId = "hello1" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-01", MessageId = "hello2" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-02", MessageId = "hello3" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-03", MessageId = "hello4" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-01", MessageId = "hello5" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-03", MessageId = "hello6" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-01", MessageId = "hello7" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-02", MessageId = "hello8" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-01", MessageId = "hello9" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-03", MessageId = "hello10" } };
            yield return new object[] { new MessageIdToStream { StreamExpected = "invoices-02", MessageId = "hello88" } };
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    [Theory]
    [ClassData(typeof(MessageIdToStreamTestCases))]
    public void ValidateHashRoutingStrategy(MessageIdToStream @msg)
    {
        // this test validates that the hash routing strategy is working as expected
        var murmurStrategy = new HashRoutingMurmurStrategy(message => message.Properties.MessageId.ToString());
        var messageTest = new Message(Encoding.Default.GetBytes("hello"))
        {
            Properties = new Properties() { MessageId = msg.MessageId }
        };
        var routes =
            murmurStrategy.Route(messageTest, new List<string>() { "invoices-01", "invoices-02", "invoices-03" });

        Assert.Single(routes);
        Assert.Equal(msg.StreamExpected, routes[0]);
    }
}
