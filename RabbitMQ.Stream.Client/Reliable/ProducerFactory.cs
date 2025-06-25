// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace RabbitMQ.Stream.Client.Reliable;

// <summary>
// ProducerFactory is a middleware component that creates a producer for a given configuration.
// There are two possible producers:
// - standard producer 
// - super stream producer
// </summary>

public abstract class ProducerFactory : ReliableBase
{
    protected IProducer _producer;
    protected ProducerConfig _producerConfig;
    protected ConfirmationPipe _confirmationPipe;

    protected async Task<IProducer> CreateProducer(bool boot)
    {
        if (_producerConfig.SuperStreamConfig is { Enabled: true })
        {
            return await SuperStreamProducer(boot).ConfigureAwait(false);
        }

        return await StandardProducer().ConfigureAwait(false);
    }

    private async Task<IProducer> SuperStreamProducer(bool boot)
    {
        if (boot)
        {
            return await _producerConfig.StreamSystem.CreateRawSuperStreamProducer(
                new RawSuperStreamProducerConfig(_producerConfig.Stream)
                {
                    ClientProvidedName = _producerConfig.ClientProvidedName,
                    Reference = _producerConfig.Reference,
                    MessagesBufferSize = _producerConfig.MessagesBufferSize,
                    MaxInFlight = _producerConfig.MaxInFlight,
                    Routing = _producerConfig.SuperStreamConfig.Routing,
                    RoutingStrategyType = _producerConfig.SuperStreamConfig.RoutingStrategyType,
                    Filter = _producerConfig.Filter,
                    Identifier = _producerConfig.Identifier,
                    ConnectionClosedHandler = async (closeReason, partitionStream) =>
                    {
                        await RandomWait().ConfigureAwait(false);
                        if (IsClosedNormally(closeReason))
                        {
                            UpdateStatus(ReliableEntityStatus.Closed, ChangeStatusReason.ClosedByUser,
                                [partitionStream]);
                            return;
                        }

                        try
                        {
                            var r = ((RawSuperStreamProducer)(_producer)).ReconnectPartition;
                            await OnEntityClosed(_producerConfig.StreamSystem, partitionStream, r,
                                    FromConnectionClosedReasonToStatusReason(closeReason))
                                .ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            BaseLogger?.LogError(e,
                                $"Super stream producer. ConnectionClosedHandler error. Auto recovery failed for stream: {_producerConfig.Stream}");
                        }
                    },
                    MetadataHandler = async update =>
                    {
                        await RandomWait().ConfigureAwait(false);
                        if (IsClosedNormally())
                            return;
                        try
                        {
                            var r = ((RawSuperStreamProducer)(_producer)).ReconnectPartition;
                            await OnEntityClosed(_producerConfig.StreamSystem, update.Stream, r,
                                    ChangeStatusReason.MetaDataUpdate)
                                .ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            BaseLogger?.LogError(e,
                                $"Super stream producer. MetadataHandler error. Auto recovery failed stream: {_producerConfig.Stream}");
                        }
                    },
                    ConfirmHandler = confirmationHandler =>
                    {
                        var (stream, confirmation) = confirmationHandler;
                        var confirmationStatus = confirmation.Code switch
                        {
                            ResponseCode.PublisherDoesNotExist => ConfirmationStatus.PublisherDoesNotExist,
                            ResponseCode.AccessRefused => ConfirmationStatus.AccessRefused,
                            ResponseCode.InternalError => ConfirmationStatus.InternalError,
                            ResponseCode.PreconditionFailed => ConfirmationStatus.PreconditionFailed,
                            ResponseCode.StreamNotAvailable => ConfirmationStatus.StreamNotAvailable,
                            ResponseCode.Ok => ConfirmationStatus.Confirmed,
                            _ => ConfirmationStatus.UndefinedError
                        };
                        _confirmationPipe.RemoveUnConfirmedMessage(confirmationStatus, confirmation.PublishingId,
                            stream).ConfigureAwait(false);
                    }
                }, BaseLogger).ConfigureAwait(false);
        }

        return _producer;
    }

    private async Task<IProducer> StandardProducer()
    {
        // before creating a new producer, the old one is disposed
        // This is just a safety check, the producer should be already disposed
        _producer?.Dispose();

        return await _producerConfig.StreamSystem.CreateRawProducer(new RawProducerConfig(_producerConfig.Stream)
        {
            ClientProvidedName = _producerConfig.ClientProvidedName,
            Reference = _producerConfig.Reference,
            MaxInFlight = _producerConfig.MaxInFlight,
            Filter = _producerConfig.Filter,
            Identifier = _producerConfig.Identifier,
            MetadataHandler = async _ =>
            {
                await RandomWait().ConfigureAwait(false);
                if (IsClosedNormally())
                {
                    UpdateStatus(ReliableEntityStatus.Closed, ChangeStatusReason.ClosedByUser,
                        [_producerConfig.Stream]);
                    return;
                }

                await OnEntityClosed(_producerConfig.StreamSystem, _producerConfig.Stream,
                    ChangeStatusReason.MetaDataUpdate).ConfigureAwait(false);
            },
            ConnectionClosedHandler = async (closeReason) =>
            {
                await RandomWait().ConfigureAwait(false);
                if (IsClosedNormally(closeReason))
                    return;

                await OnEntityClosed(_producerConfig.StreamSystem, _producerConfig.Stream,
                    ReliableBase.FromConnectionClosedReasonToStatusReason(closeReason)).ConfigureAwait(false);
            },
            ConfirmHandler = confirmation =>
            {
                var confirmationStatus = confirmation.Code switch
                {
                    ResponseCode.PublisherDoesNotExist => ConfirmationStatus.PublisherDoesNotExist,
                    ResponseCode.AccessRefused => ConfirmationStatus.AccessRefused,
                    ResponseCode.InternalError => ConfirmationStatus.InternalError,
                    ResponseCode.PreconditionFailed => ConfirmationStatus.PreconditionFailed,
                    ResponseCode.StreamNotAvailable => ConfirmationStatus.StreamNotAvailable,
                    ResponseCode.Ok => ConfirmationStatus.Confirmed,
                    _ => ConfirmationStatus.UndefinedError
                };
                _confirmationPipe.RemoveUnConfirmedMessage(confirmationStatus, confirmation.PublishingId,
                    confirmation.Stream).ConfigureAwait(false);
            }
        }, BaseLogger).ConfigureAwait(false);
    }
}
