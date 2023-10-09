// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client.EventBus;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

public enum EventTypes
{
    Connection,
    Reconnection,
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
    void Subscribe<T>(Func<T, Task> func) where T : IStreamEvent;
}

public class StreamEventsBus : IEventBus
{
    private readonly ConcurrentDictionary<Type, List<Func<IStreamEvent, Task>>> _subscriptions = new();

    public void Publish<T>(T v) where T : IStreamEvent
    {
        var type = typeof(T);
        if (_subscriptions.TryGetValue(type, out var funcs))
        {
            foreach (var func in funcs)
            {
                func(v);
            }
        }
    }

    public void Subscribe<T>(Func<T, Task> func) where T : IStreamEvent
    {
        var type = typeof(T);
        if (!_subscriptions.TryGetValue(type, out var funcs))
        {
            funcs = new List<Func<IStreamEvent, Task>>();
            _subscriptions.TryAdd(type, funcs);
        }

        funcs.Add(e => func((T)e));
    }
}

public static class StreamEventsBusSingleton
{
    private static readonly Lazy<StreamEventsBus> s_lazy = new(() => new StreamEventsBus());
    public static StreamEventsBus Instance => s_lazy.Value;
}
