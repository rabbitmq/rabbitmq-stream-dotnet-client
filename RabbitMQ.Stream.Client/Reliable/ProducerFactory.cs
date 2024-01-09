// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.


/* Unmerged change from project 'RabbitMQ.Stream.Client(net7.0)'
Before:
using System.Threading;
using System.Threading.Tasks;
After:
using System.Threading.Tasks;
*/
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
    protected ProducerConfig _producerConfig;
    protected ConfirmationPipe _confirmationPipe;

    protected async Task<IProducer> CreateProducer()
    {
        if (_producerConfig.SuperStreamConfig is { Enabled: true })
        {
            return await SuperStreamProducer().ConfigureAwait(false);
        }

        return await StandardProducer().ConfigureAwait(false);
    }

    private async Task<IProducer> SuperStreamProducer()
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

    private async Task<IProducer> StandardProducer()
    {
        return await _producerConfig.StreamSystem.CreateRawProducer(new RawProducerConfig(_producerConfig.Stream)
        {
            ClientProvidedName = _producerConfig.ClientProvidedName,
            Reference = _producerConfig.Reference,
            MaxInFlight = _producerConfig.MaxInFlight,
            Filter = _producerConfig.Filter,
            MetadataHandler = async _ =>
            {
                await OnEntityClosed(_producerConfig.StreamSystem, _producerConfig.Stream).ConfigureAwait(false);
            },
            ConnectionClosedHandler = async (closeReason) =>
            {
                if (closeReason == ConnectionClosedReason.Normal)
                {
                    BaseLogger.LogInformation("Reconnect is skipped. {Identity} is closed normally", ToString());
                    return;
                }

                await OnEntityClosed(_producerConfig.StreamSystem, _producerConfig.Stream).ConfigureAwait(false);
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
