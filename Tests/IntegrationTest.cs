// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client;
using Xunit;
using Xunit.Abstractions;
using System.Collections;

namespace Tests
{
    public class IntegrationTest
    {
        private readonly ITestOutputHelper _output;

        public IntegrationTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Theory]
        [ClassData(typeof(EventLengthTestCases))]
        public async Task TestEventLength(ReadOnlySequence<byte> @event)
        {
            var streamReference = Guid.NewGuid().ToString();
            var streamName = Guid.NewGuid().ToString();
            var system = await StreamSystem.Create(new ());
            await system.CreateStream(new StreamSpec(streamName));
            var responseCodes = new List<(ulong PublishingId, ResponseCode Code)>();

            var producer = await system.CreateProducer(new ProducerConfig
            {
                Reference = streamReference,
                Stream = streamName,
                // Here you can receive the messages confirmation
                // it means the message is stored on the server
                ConfirmHandler = conf =>
                {
                    _output.WriteLine($"Event published with PublishingId: {conf.PublishingId} - {conf.Code}");
                    responseCodes.Add((conf.PublishingId, conf.Code));
                }
            });

            var receivedMessages = new List<Message>();
            var consumer = await system.CreateConsumer(new ConsumerConfig
            {
                Reference = streamReference,
                Stream = streamName,
                // Consume the stream from the Offset
                OffsetSpec = new OffsetTypeOffset(),
                // Receive the messages
                MessageHandler = (_, _, message) =>
                {
                    receivedMessages.Add(message);
                    _output.WriteLine($"Event received with size: {message.Size} - Content Length: {message.Data.Contents.Length}");
                    return Task.CompletedTask;
                }
            });

            ulong publishingId = 0;
            var data = new Data(@event);
            await producer.Send(publishingId, new Message(data));

            // Wait for the messages to be processed.
            var messageSent = await WaitForConditionAsync(() =>
                Task.FromResult(responseCodes.Any(response =>
                    response.PublishingId == 0 && response.Code == ResponseCode.Ok)), 2500, 10);

            var messageReceived =
                await WaitForConditionAsync(() => Task.FromResult(receivedMessages.Any(m => m.Data.Contents.Length == @event.Length)), 2500, 10);

            // Clean up stream before assertion.
            await system.DeleteStream(streamName);
            await WaitForConditionAsync(async () => !await system.StreamExists(streamName), 2500, 10);
            await consumer.Close();
            await producer.Close();
            await system.Close();

            // Assert
            Assert.True(messageSent);
            Assert.True(messageReceived);
        }

        private static async Task<bool> WaitForConditionAsync(Func<Task<bool>> condition, int timeoutMs, int pollingInterval)
        {
            bool result;
            do
            {
                result = await condition();
                timeoutMs -= pollingInterval;
                await Task.Delay(pollingInterval);
            }
            while (timeoutMs > 0 && !result);

            return result;
        }

        private class EventLengthTestCases : IEnumerable<object[]>
        {
            private readonly Random _random = new(3895);

            public IEnumerator<object[]> GetEnumerator()
            {
                yield return new object[] { GetRandomBytes(254) };
                yield return new object[] { GetRandomBytes(255) };
                yield return new object[] { GetRandomBytes(256) };
            }

            private ReadOnlySequence<byte> GetRandomBytes(ulong length)
            {
                var arr = new byte[length];
                _random.NextBytes(arr);
                return new ReadOnlySequence<byte>(arr);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
