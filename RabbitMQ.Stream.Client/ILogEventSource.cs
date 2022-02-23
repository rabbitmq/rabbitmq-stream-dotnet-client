// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client
{
    internal interface ILogEventSource
    {
        /// <summary>
        /// Writes an informational log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        ILogEventSource LogInformation(string message);

        /// <summary>
        /// Writes an informational log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        /// 
        /// <param name="args">
        /// </param>
        ILogEventSource LogInformation(string message, params object[] args);

        /// <summary>
        /// Writes a warning log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        ILogEventSource LogWarning(string message);

        /// <summary>
        /// Writes a warning log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        /// 
        /// <param name="args">
        /// </param>
        ILogEventSource LogWarning(string message, params object[] args);

        /// <summary>
        /// Writes an error log message.
        /// </summary>
        /// 
        /// <param name="message">
        /// </param>
        ILogEventSource LogError(string message);

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
        ILogEventSource LogError(string message, Exception exception);

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
        ILogEventSource LogError(string message, Exception exception, params object[] args);
    }
}
