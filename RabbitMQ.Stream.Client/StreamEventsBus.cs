// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client;

public enum EventTypes
{
    Connection,
    Disconnection,
}

public enum EventSeverity
{
    Info,
    Warning,
    Error,
}

public interface IStreamEvent
{
    EventTypes EventType { get; }
    EventSeverity EventSeverity { get; }
}

public interface IEventBus
{
    void Publish<T>(T v) where T : IStreamEvent;
    void Subscribe<T>(Action<T> action) where T : IStreamEvent;
}

public class StreamEventsBus : IEventBus
{
    private readonly ConcurrentDictionary<Type, List<Action<IStreamEvent>>> _subscriptions = new();

    public void Publish<T>(T v) where T : IStreamEvent
    {
        var type = typeof(T);
        if (_subscriptions.TryGetValue(type, out var actions))
        {
            foreach (var action in actions)
            {
                action(v);
            }
        }
    }

    public void Subscribe<T>(Action<T> action) where T : IStreamEvent
    {
        var type = typeof(T);
        if (!_subscriptions.TryGetValue(type, out var actions))
        {
            actions = new List<Action<IStreamEvent>>();
            _subscriptions.TryAdd(type, actions);
        }

        actions.Add(e => action((T)e));
    }
}

public class StreamEventsBusSingleton
{
    private static readonly Lazy<StreamEventsBus> s_lazy = new(() => new StreamEventsBus());
    public static StreamEventsBus Instance => s_lazy.Value;
}
