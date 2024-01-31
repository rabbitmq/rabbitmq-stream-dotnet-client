// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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
    // When the Filter is set also Values must be set and PostFilter must be set
    // Values must be a list of string and must contain at least one element
    [SkippableFact]
    public async void ValidateFilter()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        if (!AvailableFeaturesSingleton.Instance.PublishFilter)
        {
            throw new SkipException("broker does not support filter");
        }

        await Assert.ThrowsAsync<ArgumentException>(() => Consumer.Create(
            new ConsumerConfig(system, stream) { Filter = new ConsumerFilter() }
        ));

        await Assert.ThrowsAsync<ArgumentException>(() => Consumer.Create(
            new ConsumerConfig(system, stream) { Filter = new ConsumerFilter() { Values = new List<string>() } }
        ));

        await Assert.ThrowsAsync<ArgumentException>(() => Consumer.Create(new ConsumerConfig(system, stream)
        {
            Filter = new ConsumerFilter() { Values = new List<string>() { "test" }, PostFilter = null }
        }
        ));

        await Assert.ThrowsAsync<ArgumentException>(() => Consumer.Create(
            new ConsumerConfig(system, stream)
            {
                Filter = new ConsumerFilter() { Values = new List<string>(), PostFilter = _ => true }
            }
        ));

        var c = await Consumer.Create(new ConsumerConfig(system, stream));
        Assert.NotNull(c);
        await c.Close();
        await SystemUtils.CleanUpStreamSystem(system, stream).ConfigureAwait(false);
    }

    // This test is checking that the filter is working as expected
    // We send 100 messages with two different states (Alabama and New York)
    // By using the filter we should be able to consume only the messages from Alabama 
    // and the server has to send only one chunk with all the messages from Alabama
    [SkippableFact]
    public async void FilterShouldReturnOnlyOneChunk()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        if (!AvailableFeaturesSingleton.Instance.PublishFilter)
        {
            throw new SkipException("broker does not support filter");
        }

        var producer = await Producer.Create(
            new ProducerConfig(system, stream)
            {
                Filter = new ProducerFilter()
                {
                    // define the producer filter 
                    FilterValue = message => message.ApplicationProperties["state"].ToString(),
                }
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
            Filter = new ConsumerFilter()
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
            Filter = new ConsumerFilter()
            {
                Values = new List<string>() { "New York" },
                PostFilter =
                    message => message.Properties.GroupId.ToString()!
                        .Equals("group_25"), // we only want the message with  group_25 ignoring the rest
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

    // This test is to test when there are errors on the filter functions
    // producer side and consumer side. 
    // FilterValue and PostFilter are user's functions and can throw exceptions
    // The client must handle those exceptions and report them to the user
    // For the producer side we have the ConfirmationHandler the messages with errors 
    // will be reported as not confirmed and the user can handle them.
    // for the consumer the messages will be skipped and logged with the standard logger
    [SkippableFact]
    public async void ErrorFiltersFunctionWontDeliverTheMessage()
    {
        SystemUtils.InitStreamSystemWithRandomStream(out var system, out var stream);
        if (!AvailableFeaturesSingleton.Instance.PublishFilter)
        {
            throw new SkipException("broker does not support filter");
        }

        var messagesConfirmed = 0;
        var messagesError = 0;
        var testPassed = new TaskCompletionSource<int>();
        const int ToSend = 5;

        var producer = await Producer.Create(
            new ProducerConfig(system, stream)
            {
                TimeoutMessageAfter = TimeSpan.FromSeconds(2),
                ConfirmationHandler = async confirmation =>
                {
                    if (confirmation.Status == ConfirmationStatus.Confirmed)
                        messagesConfirmed++;
                    else
                        messagesError++; // we should have only one error caused by the message with id_4 On FilterValue function

                    if (messagesConfirmed + messagesError == ToSend)
                    {
                        testPassed.SetResult(ToSend);
                    }

                    await Task.CompletedTask.ConfigureAwait(false);
                },
                // define the producer filter 
                Filter = new ProducerFilter()
                {
                    FilterValue = message =>
                    {
                        if (message.Properties.MessageId!.Equals("id_4"))
                        {
                            // we simulate an error on the filter function
                            // the message with id_4 will be reported as not confirmed
                            throw new Exception("Simulate an error");
                        }

                        return "my_filter";
                    }
                }
            }
        );

        for (var i = 0; i < ToSend; i++)
        {
            await producer.Send(new Message(Encoding.UTF8.GetBytes("Message: " + i))
            {
                Properties = new Properties() { MessageId = $"id_{i}" }
            }).ConfigureAwait(false);
        }

        Assert.True(testPassed.Task.Wait(TimeSpan.FromSeconds(5)));
        // we should have 4 messages confirmed and 1 error == 5
        // since we are filtering the message with id_4 and throwing an exception
        Assert.Equal(ToSend - 1, messagesConfirmed);
        Assert.Equal(1, messagesError);

        var consumed = new List<Message>();
        var consumer = await Consumer.Create(new ConsumerConfig(system, stream)
        {
            OffsetSpec = new OffsetTypeFirst(),
            Filter = new ConsumerFilter()
            {
                Values = new List<string>() { "my_filter" },// at this level we don't care about the filter value
                PostFilter =
                    message =>
                    {
                        // We simulate an error on the post filter function
                        if (message.Properties.MessageId!.Equals("id_2"))
                            throw new Exception("Simulate an error");

                        return true;
                    },
                MatchUnfiltered = true
            },
            MessageHandler = (_, _, _, message) =>
            {
                consumed.Add(message);
                // the message message.Properties.MessageId!.Equals("id_2") will be skipped
                return Task.CompletedTask;
            }
        }).ConfigureAwait(false);

        SystemUtils.Wait(TimeSpan.FromSeconds(3));
        // we should have 3 messages since there is an error in the PostFilter
        // function for the message with id_2
        // So we sent 5 messages. 1 error was thrown in the producer filter and 1 error in the consumer Postfilter
        Assert.Equal(3, consumed.Count);

        // No message with id_2 should be consumed, since we simulate and error
        // during the post filter function
        Assert.Empty(consumed.Where(message => message.Properties.MessageId!.Equals("id_2")).ToList());
        await producer.Close().ConfigureAwait(false);
        await consumer.Close().ConfigureAwait(false);
        await SystemUtils.CleanUpStreamSystem(system, stream).ConfigureAwait(false);
    }
}
