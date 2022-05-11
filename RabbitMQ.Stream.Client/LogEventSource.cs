// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Diagnostics.Tracing;

namespace RabbitMQ.Stream.Client
{
    public static class Keywords
    {
        public const EventKeywords Log = (EventKeywords)1;
    }

    [EventSource(Name = "rabbitmq-client-stream")]
    internal sealed class LogEventSource : EventSource, ILogEventSource
    {
        /// <summary>
        /// Default <see cref="LogEventSource" /> implementation for logging.
        /// </summary>
        public static readonly
            ILogEventSource Log = new
             LogEventSource
            ();

        private LogEventSource() : base(EventSourceSettings.EtwSelfDescribingEventFormat)
        { }

        /// <summary>
        /// </summary>
        [NonEvent]
        private static string ConvertToString(Exception exception)
        {
            return exception == default ? default : $"{Environment.NewLine}{exception?.ToString()}";
        }

        /// <summary>
        /// Writes an informational log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        [Event(1, Message = "INFO", Keywords = Keywords.Log, Level = EventLevel.Informational)]
        public ILogEventSource LogInformation(string message)
        {
            if (IsEnabled())
            {
                WriteEvent(1, message);
            }

            return this;
        }

        /// <summary>
        /// Writes an informational log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        /// 
        /// <param name="args">
        /// </param>
        [NonEvent]
        public ILogEventSource LogInformation(string message, params object[] args)
        {
            return LogInformation(string.Format(message, args));
        }

        /// <summary>
        /// Writes a warning log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        [Event(2, Message = "WARN", Keywords = Keywords.Log, Level = EventLevel.Warning)]
        public ILogEventSource LogWarning(string message)
        {
            if (IsEnabled())
            {
                WriteEvent(2, message);
            }

            return this;
        }

        /// <summary>
        /// Writes a warning log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        /// 
        /// <param name="args">
        /// </param>
        public ILogEventSource LogWarning(string message, params object[] args)
        {
            return LogWarning(string.Format(message, args));
        }

        /// <summary>
        /// Writes an error log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        [Event(3, Message = "ERROR", Keywords = Keywords.Log, Level = EventLevel.Error)]
        public ILogEventSource LogError(string message)
        {
            if (IsEnabled())
            {
                WriteEvent(3, message);
            }

            return this;
        }

        /// <summary>
        /// Writes an error log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        /// 
        /// <param name="exception">
        /// The exception to log.
        /// </param>
        [NonEvent]
        public ILogEventSource LogError(string message, Exception exception)
        {
            LogError($"{message}{ConvertToString(exception)}");

            return this;
        }

        /// <summary>
        /// Writes an error log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        /// 
        /// <param name="exception">
        /// The exception to log.
        /// </param>
        /// 
        /// <param name="args">
        /// </param>
        [NonEvent]
        public ILogEventSource LogError(string message, Exception exception, params object[] args)
        {
            return LogError(string.Format(message, args), exception);
        }
    }
}
