﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace RabbitMQ.Stream.Client.Reliable;

/// <summary>
/// ConsumerFactory is the class to decide which kind on the consumer is needed
/// Consumers are: Standard Consumer and Super Stream Consumer
/// </summary>
public abstract class ConsumerFactory : ReliableBase
{
    protected ConsumerConfig _consumerConfig;
    protected IConsumer _consumer;

    // this list contains the map between the stream and last consumed offset 
    // standard consumer is just one 
    // super stream consumer is one per stream-partition
    private readonly ConcurrentDictionary<string, ulong> _lastOffsetConsumed = new();
    private bool _consumedFirstTime;

    protected async Task<IConsumer> CreateConsumer(bool boot)
    {
        if (_consumerConfig.IsSuperStream)
        {
            return await SuperConsumer(boot).ConfigureAwait(false);
        }

        return await StandardConsumer(boot).ConfigureAwait(false);
    }

    private async Task<IConsumer> StandardConsumer(bool boot)
    {
        var offsetSpec = _consumerConfig.OffsetSpec;
        // if is not the boot time and at least one message was consumed
        // it can restart consuming from the last consumer offset + 1 (+1 since we need to consume from the next)
        if (!boot && _consumedFirstTime)
        {
            offsetSpec = new OffsetTypeOffset(_lastOffsetConsumed[_consumerConfig.Stream] + 1);
        }

        // before creating a new consumer, the old one is disposed
        // This is just a safety check, the consumer should be already disposed
        _consumer?.Dispose();
        return await _consumerConfig.StreamSystem.CreateRawConsumer(new RawConsumerConfig(_consumerConfig.Stream)
        {
            ClientProvidedName = _consumerConfig.ClientProvidedName,
            Reference = _consumerConfig.Reference,
            ConsumerUpdateListener = _consumerConfig.ConsumerUpdateListener,
            IsSingleActiveConsumer = _consumerConfig.IsSingleActiveConsumer,
            InitialCredits = _consumerConfig.InitialCredits,
            OffsetSpec = offsetSpec,
            ConsumerFilter = _consumerConfig.Filter,
            Crc32 = _consumerConfig.Crc32,
            Identifier = _consumerConfig.Identifier,
            FlowControl = _consumerConfig.FlowControl,
            ConnectionClosedHandler = async (closeReason) =>
            {
                if (IsClosedNormally(closeReason))
                {
                    UpdateStatus(ReliableEntityStatus.Closed, ChangeStatusReason.ClosedByUser,
                        [_consumerConfig.Stream]);
                    return;
                }

                try
                {
                    await OnEntityClosed(_consumerConfig.StreamSystem, _consumerConfig.Stream,
                        FromConnectionClosedReasonToStatusReason(closeReason)).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    BaseLogger?.LogError(e,
                        $"Stream consumer.ConnectionClosedHandler error. Auto recovery failed for: {_consumerConfig.Stream}");
                }
            },
            MetadataHandler = async _ =>
            {
                if (IsClosedNormally())
                    return;

                try
                {
                    await OnEntityClosed(_consumerConfig.StreamSystem, _consumerConfig.Stream,
                        ChangeStatusReason.MetaDataUpdate).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    BaseLogger?.LogError(e,
                        $"Stream consumer.MetadataHandler error. Auto recovery failed for stream: {_consumerConfig.Stream}");
                }
            },
            MessageHandler = async (consumer, ctx, message) =>
            {
                try
                {
                    if (_consumerConfig.MessageHandler != null)
                    {
                        await _consumerConfig.MessageHandler(_consumerConfig.Stream, consumer, ctx, message)
                            .ConfigureAwait(false);
                    }

                    _consumedFirstTime = true;
                }
                catch (Exception e)
                {
                    BaseLogger?.LogError("MessageHandler {Error} for stream {Stream} ", e.Message,
                        _consumerConfig.Stream);
                }

                _lastOffsetConsumed[_consumerConfig.Stream] = ctx.Offset;
            },
        }, BaseLogger).ConfigureAwait(false);
    }

    private async Task<IConsumer> SuperConsumer(bool boot)
    {
        ConcurrentDictionary<string, IOffsetType> offsetSpecs = new();
        // if is not the boot time and at least one message was consumed
        // it can restart consuming from the last consumer offset + 1 (+1 since we need to consume from the next)
        if (!boot && _consumedFirstTime)
        {
            foreach (var (streamOff, offset) in _lastOffsetConsumed)
            {
                offsetSpecs[streamOff] = new OffsetTypeOffset(offset + 1);
            }
        }
        else
        {
            var partitions = await _consumerConfig.StreamSystem.QueryPartition(_consumerConfig.Stream)
                .ConfigureAwait(false);
            foreach (var partition in partitions)
            {
                offsetSpecs[partition] =
                    _consumerConfig.OffsetSpec;
            }
        }

        if (boot)
        {
            return await _consumerConfig.StreamSystem.CreateSuperStreamConsumer(
                new RawSuperStreamConsumerConfig(_consumerConfig.Stream)
                {
                    ClientProvidedName = _consumerConfig.ClientProvidedName,
                    Reference = _consumerConfig.Reference,
                    ConsumerUpdateListener = _consumerConfig.ConsumerUpdateListener,
                    IsSingleActiveConsumer = _consumerConfig.IsSingleActiveConsumer,
                    InitialCredits = _consumerConfig.InitialCredits,
                    ConsumerFilter = _consumerConfig.Filter,
                    Crc32 = _consumerConfig.Crc32,
                    OffsetSpec = offsetSpecs,
                    Identifier = _consumerConfig.Identifier,
                    FlowControl = _consumerConfig.FlowControl,
                    ConnectionClosedHandler = async (closeReason, partitionStream) =>
                    {
                        if (IsClosedNormally(closeReason))
                        {
                            UpdateStatus(ReliableEntityStatus.Closed, ChangeStatusReason.ClosedByUser,
                                [partitionStream]);
                            return;
                        }

                        await RandomWait().ConfigureAwait(false);
                        try
                        {
                            var r = ((RawSuperStreamConsumer)(_consumer)).ReconnectPartition;
                            await OnEntityClosed(_consumerConfig.StreamSystem, partitionStream, r,
                                FromConnectionClosedReasonToStatusReason(closeReason)).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            BaseLogger?.LogError(e,
                                $"Super stream consumer. ConnectionClosedHandler error. Auto recovery failed for stream: {_consumerConfig.Stream}");
                        }
                    },
                    MetadataHandler = async update =>
                    {
                        try
                        {
                            if (IsClosedNormally())
                                return;
                            await RandomWait().ConfigureAwait(false);

                            var r = ((RawSuperStreamConsumer)(_consumer)).ReconnectPartition;
                            await OnEntityClosed(_consumerConfig.StreamSystem, update.Stream, r,
                                    ChangeStatusReason.MetaDataUpdate)
                                .ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            BaseLogger?.LogError(e,
                                $"Super stream consumer.MetadataHandler error. Auto recovery failed stream: {_consumerConfig.Stream}");
                        }
                    },
                    MessageHandler = async (partitionStream, consumer, ctx, message) =>
                    {
                        try
                        {
                            if (_consumerConfig.MessageHandler != null)
                            {
                                await _consumerConfig.MessageHandler(partitionStream, consumer, ctx,
                                    message).ConfigureAwait(false);
                            }

                            _consumedFirstTime = true;
                            _lastOffsetConsumed[_consumerConfig.Stream] = ctx.Offset;
                        }
                        catch (Exception e)
                        {
                            BaseLogger?.LogError("MessageHandler {Error} for stream {Stream} ", e.Message,
                                _consumerConfig.Stream);
                        }
                    },
                }, BaseLogger).ConfigureAwait(false);
        }

        return _consumer;
    }
}
