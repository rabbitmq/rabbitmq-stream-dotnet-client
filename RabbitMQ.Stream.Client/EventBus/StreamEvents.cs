// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Collections.Generic;
using RabbitMQ.Stream.Client.Reliable;

namespace RabbitMQ.Stream.Client.EventBus;

public abstract class ClientEvent : IStreamEvent
{
    protected ClientEvent(IDictionary<string, string> connectionProperties, ClientParameters parameters,
        EventTypes eventType, EventSeverity eventSeverity)
    {
        ConnectionProperties = connectionProperties;
        Parameters = parameters;
        EventType = eventType;
        EventSeverity = eventSeverity;
    }

    public EventTypes EventType { get; internal set; }
    public EventSeverity EventSeverity { get; internal set; }

    public ClientParameters Parameters { get; }
    public IDictionary<string, string> ConnectionProperties { get; }
}

public class RawProducerConnected : ClientEvent
{
    public RawProducerConnected(IDictionary<string, string> connectionProperties, ClientParameters parameters,
        RawProducer instance)
        : base(connectionProperties, parameters, EventTypes.Connection, EventSeverity.Info)
    {
        Instance = instance;
    }

    public RawProducer Instance { get; }
}

public class RawProducerDisconnected : ClientEvent
{
    public RawProducerDisconnected(IDictionary<string, string> connectionProperties,
        ClientParameters parameters, RawProducer instance)
        : base(connectionProperties, parameters, EventTypes.Disconnection, EventSeverity.Info)
    {
        Instance = instance;
    }

    public RawProducer Instance { get; }
}

public class ReliableBaseReconnected : IStreamEvent
{
    public ReliableBaseReconnected(bool isReconnection, EventSeverity eventSeverity)
    {
        IsReconnection = isReconnection;
        EventSeverity = eventSeverity;
    }

    public bool IsReconnection { get; }
    public EventTypes EventType { get; } = EventTypes.Reconnection;
    public EventSeverity EventSeverity { get; }
}

public class ProducerReconnected : ReliableBaseReconnected
{
    public Producer Instance { get; }

    public ProducerReconnected(bool isReconnection, EventSeverity eventSeverity, Producer instance) : base(
        isReconnection, eventSeverity)
    {
        Instance = instance;
    }
}
