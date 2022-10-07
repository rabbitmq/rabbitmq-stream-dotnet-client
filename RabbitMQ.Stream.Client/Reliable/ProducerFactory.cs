// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.Reliable;

// <summary>
// ProducerFactory is a middleware component that creates a producer for a given configuration.
// There are two possible producers:
// - standard producer 
// - super stream producer
// </summary>

public abstract class ProducerFactory : ReliableBase
{
    protected ReliableProducerConfig _reliableProducerConfig;
    protected ConfirmationPipe _confirmationPipe;

    protected async Task<IProducer> CreateProducer()
    {
        if (_reliableProducerConfig.SuperStreamConfig is { Enabled: true })
        {
            return await SuperStreamProducer();
        }

        return await StandardProducer();
    }

    private async Task<IProducer> SuperStreamProducer()
    {
        return await _reliableProducerConfig.StreamSystem.CreateSuperStreamProducer(new SuperStreamProducerConfig()
        {
            SuperStream = _reliableProducerConfig.Stream,
            ClientProvidedName = _reliableProducerConfig.ClientProvidedName,
            Reference = _reliableProducerConfig.Reference,
            MaxInFlight = _reliableProducerConfig.MaxInFlight,
            Routing = _reliableProducerConfig.SuperStreamConfig.Routing,
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
                    stream);
            }
        });
    }

    private async Task<IProducer> StandardProducer()
    {
        return await _reliableProducerConfig.StreamSystem.CreateProducer(new ProducerConfig()
        {
            Stream = _reliableProducerConfig.Stream,
            ClientProvidedName = _reliableProducerConfig.ClientProvidedName,
            Reference = _reliableProducerConfig.Reference,
            MaxInFlight = _reliableProducerConfig.MaxInFlight,
            MetadataHandler = update =>
            {
                // This is Async since the MetadataHandler is called from the Socket connection thread
                // HandleMetaDataMaybeReconnect/2 could go in deadlock.

                Task.Run(() =>
                {
                    // intentionally fire & forget
                    HandleMetaDataMaybeReconnect(update.Stream,
                        _reliableProducerConfig.StreamSystem).WaitAsync(CancellationToken.None);
                });
            },
            ConnectionClosedHandler = async _ => { await TryToReconnect(_reliableProducerConfig.ReconnectStrategy); },
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
                    confirmation.Stream);
            }
        });
    }
}
