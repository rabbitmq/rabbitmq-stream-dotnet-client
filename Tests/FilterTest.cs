// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using RabbitMQ.Stream.Client.Reliable;
using Xunit;

namespace Tests;

public class FilterTest
{
    [Fact]
    public async void FilterShouldReturnOnlyOneChuck()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);

        var producer = await Producer.Create(
            new ProducerConfig(system, stream)
            {
                FilterValue = message => message.ApplicationProperties["state"].ToString(),
            }
        );

        const int ToSend = 50;

        async Task SendTo(string state)
        {
            var messages = new List<Message>();
            for (var i = 0; i < ToSend; i++)
            {
                var message = new Message(Encoding.UTF8.GetBytes($"Message: {i}.  State: {state}"))
                {
                    ApplicationProperties = new ApplicationProperties() { ["state"] = state },
                    Properties = new Properties() { GroupId = $"group_{i}" }
                };
                await producer.Send(message).ConfigureAwait(false);
                messages.Add(message);
            }

            await producer.Send(messages).ConfigureAwait(false);
        }

        await SendTo("Alabama");
        await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        await SendTo("New York");
        await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);

        var testPassedAlabama = new TaskCompletionSource<int>();
        var consumedAlabama = new List<Message>();
        var consumerAlabama = await Consumer.Create(new ConsumerConfig(system, stream)
        {
            OffsetSpec = new OffsetTypeFirst(),

            // This is mandatory for enabling the filter
            Filter = new Filter()
            {
                Values = new List<string>() { "Alabama" },
                PostFilter =
                    _ =>
                        true, // we don't apply any post filter here to be sure that the server is doing the filtering 
                MatchUnfiltered = true
            },
            MessageHandler = (_, _, _, message) =>
            {
                consumedAlabama.Add(message);
                if (consumedAlabama.Count == ToSend * 2)
                {
                    testPassedAlabama.SetResult(ToSend * 2);
                }

                return Task.CompletedTask;
            }
        }).ConfigureAwait(false);
        Assert.True(testPassedAlabama.Task.Wait(TimeSpan.FromSeconds(5)));

        Assert.Equal(ToSend * 2, consumedAlabama.Count);

        // check that only the messages from Alabama were
        consumedAlabama.Where(m => m.ApplicationProperties["state"].Equals("Alabama")).ToList().ForEach(m =>
        {
            Assert.Equal("Alabama", m.ApplicationProperties["state"]);
        });

        await consumerAlabama.Close().ConfigureAwait(false);
        // let's reset 
        var consumedNY = new List<Message>();

        var consumerNY = await Consumer.Create(new ConsumerConfig(system, stream)
        {
            OffsetSpec = new OffsetTypeFirst(),

            // This is mandatory for enabling the filter
            Filter = new Filter()
            {
                Values = new List<string>() { "New York" },
                PostFilter =
                    message => message.Properties.GroupId.ToString()!.Equals("group_25"), // we only want the message with  group_25 ignoring the rest
                                                                                          // this filter is client side. We should have two messages with group_25
                                                                                          // One for the standard send and one for the batch send
                MatchUnfiltered = true
            },
            MessageHandler = (_, _, _, message) =>
            {
                consumedNY.Add(message);
                return Task.CompletedTask;
            }
        }).ConfigureAwait(false);

        SystemUtils.Wait(TimeSpan.FromSeconds(1));
        Assert.Equal(2, consumedNY.Count);
        Assert.Equal("group_25", consumedNY[0].Properties.GroupId!);
        Assert.Equal("group_25", consumedNY[1].Properties.GroupId!);
        await consumerNY.Close().ConfigureAwait(false);
        await SystemUtils.CleanUpStreamSystem(system, stream).ConfigureAwait(false);
    }
}
