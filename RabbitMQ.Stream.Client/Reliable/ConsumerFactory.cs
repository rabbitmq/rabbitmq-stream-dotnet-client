// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

/// <summary>
/// ConsumerFactory is the class to decide which kind on the consumer is needed
/// Consumers are: Standard Consumer and Super Stream Consumer
/// </summary>
public abstract class ConsumerFactory : ReliableBase
{
    protected ReliableConsumerConfig _reliableConsumerConfig;
    // this list contains the map between the stream and last consumed offset 
    // standard consumer is just one 
    // super stream consumer is one per stream-partition
    private readonly ConcurrentDictionary<string, ulong> _lastOffsetConsumed = new();
    private bool _consumedFirstTime = false;

    protected async Task<IConsumer> CreateConsumer(bool boot)
    {
        if (_reliableConsumerConfig.IsSuperStream)
        {
            return await SuperConsumer(boot);
        }

        return await StandardConsumer(boot);
    }

    private async Task<IConsumer> StandardConsumer(bool boot)
    {
        var offsetSpec = _reliableConsumerConfig.OffsetSpec;
        // if is not the boot time and at least one message was consumed
        // it can restart consuming from the last consumer offset + 1 (+1 since we need to consume fro the next)
        if (!boot && _consumedFirstTime)
        {
            offsetSpec = new OffsetTypeOffset(_lastOffsetConsumed[_reliableConsumerConfig.Stream] + 1);
        }

        return await _reliableConsumerConfig.StreamSystem.CreateConsumer(new ConsumerConfig()
        {
            Stream = _reliableConsumerConfig.Stream,
            ClientProvidedName = _reliableConsumerConfig.ClientProvidedName,
            Reference = _reliableConsumerConfig.Reference,
            ConsumerUpdateListener = _reliableConsumerConfig.ConsumerUpdateListener,
            IsSingleActiveConsumer = _reliableConsumerConfig.IsSingleActiveConsumer,
            OffsetSpec = offsetSpec,
            ConnectionClosedHandler = async _ =>
            {
                await TryToReconnect(_reliableConsumerConfig.ReconnectStrategy);
            },
            MetadataHandler = update =>
            {
                // This is Async since the MetadataHandler is called from the Socket connection thread
                // HandleMetaDataMaybeReconnect/2 could go in deadlock.
                Task.Run(() =>
                {
                    HandleMetaDataMaybeReconnect(update.Stream,
                        _reliableConsumerConfig.StreamSystem).WaitAsync(CancellationToken.None);
                });
            },
            MessageHandler = async (consumer, ctx, message) =>
            {
                _consumedFirstTime = true;
                _lastOffsetConsumed[_reliableConsumerConfig.Stream] = ctx.Offset;
                if (_reliableConsumerConfig.MessageHandler != null)
                {
                    await _reliableConsumerConfig.MessageHandler(_reliableConsumerConfig.Stream, consumer, ctx,
                        message);
                }
            },
        });
    }

    private async Task<IConsumer> SuperConsumer(bool boot)
    {
        ConcurrentDictionary<string, IOffsetType> offsetSpecs = new();
        // if is not the boot time and at least one message was consumed
        // it can restart consuming from the last consumer offset + 1 (+1 since we need to consume fro the next)
        if (!boot && _consumedFirstTime)
        {
            for (var i = 0; i < _lastOffsetConsumed.Count; i++)
            {
                offsetSpecs[_lastOffsetConsumed.Keys.ElementAt(i)] =
                    new OffsetTypeOffset(_lastOffsetConsumed.Values.ElementAt(i) + 1);
            }
        }
        else
        {
            var partitions = await _reliableConsumerConfig.StreamSystem.QueryPartition(_reliableConsumerConfig.Stream);
            foreach (var partition in partitions)
            {
                offsetSpecs[partition] =
                    _reliableConsumerConfig.OffsetSpec;
            }
        }

        return await _reliableConsumerConfig.StreamSystem.CreateSuperStreamConsumer(new SuperStreamConsumerConfig()
        {
            SuperStream = _reliableConsumerConfig.Stream,
            ClientProvidedName = _reliableConsumerConfig.ClientProvidedName,
            Reference = _reliableConsumerConfig.Reference,
            ConsumerUpdateListener = _reliableConsumerConfig.ConsumerUpdateListener,
            IsSingleActiveConsumer = _reliableConsumerConfig.IsSingleActiveConsumer,
            OffsetSpec = offsetSpecs,
            MessageHandler = async (stream, consumer, ctx, message) =>
            {
                _consumedFirstTime = true;
                _lastOffsetConsumed[_reliableConsumerConfig.Stream] = ctx.Offset;
                if (_reliableConsumerConfig.MessageHandler != null)
                {
                    await _reliableConsumerConfig.MessageHandler(stream, consumer, ctx,
                        message);
                }
            },
        });
    }
}
