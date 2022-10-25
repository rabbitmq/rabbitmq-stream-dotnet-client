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
    protected ConsumerConfig _consumerConfig;
    // this list contains the map between the stream and last consumed offset 
    // standard consumer is just one 
    // super stream consumer is one per stream-partition
    private readonly ConcurrentDictionary<string, ulong> _lastOffsetConsumed = new();
    private bool _consumedFirstTime = false;

    protected async Task<IConsumer> CreateConsumer(bool boot)
    {
        if (_consumerConfig.IsSuperStream)
        {
            return await SuperConsumer(boot);
        }

        return await StandardConsumer(boot);
    }

    private async Task<IConsumer> StandardConsumer(bool boot)
    {
        var offsetSpec = _consumerConfig.OffsetSpec;
        // if is not the boot time and at least one message was consumed
        // it can restart consuming from the last consumer offset + 1 (+1 since we need to consume fro the next)
        if (!boot && _consumedFirstTime)
        {
            offsetSpec = new OffsetTypeOffset(_lastOffsetConsumed[_consumerConfig.Stream] + 1);
        }

        return await _consumerConfig.StreamSystem.CreateRawConsumer(new RawConsumerConfig(_consumerConfig.Stream)
        {
            ClientProvidedName = _consumerConfig.ClientProvidedName,
            Reference = _consumerConfig.Reference,
            ConsumerUpdateListener = _consumerConfig.ConsumerUpdateListener,
            IsSingleActiveConsumer = _consumerConfig.IsSingleActiveConsumer,
            OffsetSpec = offsetSpec,
            ConnectionClosedHandler = async _ =>
            {
                await TryToReconnect(_consumerConfig.ReconnectStrategy);
            },
            MetadataHandler = update =>
            {
                // This is Async since the MetadataHandler is called from the Socket connection thread
                // HandleMetaDataMaybeReconnect/2 could go in deadlock.
                Task.Run(() =>
                {
                    HandleMetaDataMaybeReconnect(update.Stream,
                        _consumerConfig.StreamSystem).WaitAsync(CancellationToken.None);
                });
            },
            MessageHandler = async (consumer, ctx, message) =>
            {
                _consumedFirstTime = true;
                _lastOffsetConsumed[_consumerConfig.Stream] = ctx.Offset;
                if (_consumerConfig.MessageHandler != null)
                {
                    await _consumerConfig.MessageHandler(_consumerConfig.Stream, consumer, ctx,
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
            var partitions = await _consumerConfig.StreamSystem.QueryPartition(_consumerConfig.Stream);
            foreach (var partition in partitions)
            {
                offsetSpecs[partition] =
                    _consumerConfig.OffsetSpec;
            }
        }

        return await _consumerConfig.StreamSystem.CreateSuperStreamConsumer(new SuperStreamConsumerConfig()
        {
            SuperStream = _consumerConfig.Stream,
            ClientProvidedName = _consumerConfig.ClientProvidedName,
            Reference = _consumerConfig.Reference,
            ConsumerUpdateListener = _consumerConfig.ConsumerUpdateListener,
            IsSingleActiveConsumer = _consumerConfig.IsSingleActiveConsumer,
            OffsetSpec = offsetSpecs,
            MessageHandler = async (stream, consumer, ctx, message) =>
            {
                _consumedFirstTime = true;
                _lastOffsetConsumed[_consumerConfig.Stream] = ctx.Offset;
                if (_consumerConfig.MessageHandler != null)
                {
                    await _consumerConfig.MessageHandler(stream, consumer, ctx,
                        message);
                }
            },
        });
    }
}
