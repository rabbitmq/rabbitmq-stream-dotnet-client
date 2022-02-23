// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Diagnostics.Tracing;

namespace RabbitMQ.Stream.Client
{
    public sealed class LogEventListener : EventListener, IDisposable
    {
        public LogEventListener()
        {
            EnableEvents((EventSource)LogEventSource.Log, EventLevel.Verbose, Keywords.Log);
        }

        /// <summary>
        /// Performs application-defined tasks
        /// associated with freeing, releasing,
        /// or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            DisableEvents((EventSource)LogEventSource.Log);
        }

        /// <summary>
        /// Called whenever an event has been written by
        /// an event source for which the event listener
        /// has enabled events.
        /// </summary>
        /// 
        /// <param name="eventData">
        /// The event arguments that describe the event.
        /// </param>
        protected override void OnEventWritten(EventWrittenEventArgs eventData)
        {
            for (var i = 0; i < eventData.Payload.Count; i++)
            {
                Console.WriteLine("{0}: {1}: {2}", eventData.Level, eventData.Message, eventData.Payload[i]);
            }

            base.OnEventWritten(eventData);
        }
    }
}
