// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Net.Sockets;

namespace RabbitMQ.Stream.Client
{
    /// <summary>
    /// Configurable TCP socket options for a connection. Use this to tune buffer sizes,
    /// Nagle's algorithm, and linger on close.
    /// </summary>
    public class SocketOptions
    {
        /// <summary>
        /// Default multiplier applied to the system send/receive buffer size when not set explicitly.
        /// </summary>
        internal const int DefaultBufferSizeMultiplier = 10;

        /// <summary>
        /// Send buffer size in bytes. When null, the system default multiplied by
        /// <see cref="DefaultBufferSizeMultiplier"/> is used.
        /// </summary>
        public int? SendBufferSize { get; set; }

        /// <summary>
        /// Receive buffer size in bytes. When null, the system default multiplied by
        /// <see cref="DefaultBufferSizeMultiplier"/> is used.
        /// </summary>
        public int? ReceiveBufferSize { get; set; }

        /// <summary>
        /// Disable Nagle's algorithm when true (default), reducing latency for small messages.
        /// </summary>
        public bool NoDelay { get; set; } = true;

        // don't use it.
        // It is not needed since the library will automatically reconnect in case of connection failure.
        // The property is here to avoid breaking changes.
        // mark as deprecated since it is not needed and it can be misleading for the user.
        [Obsolete("KeepAlive is not needed since the library will automatically reconnect in case of connection failure.")]
        public bool KeepAlive { get; set; } = true;

        /// <summary>
        /// Linger option on close. When set, controls whether the socket waits for unsent data
        /// and for how long (seconds). When null, the system default is used (typically abort on close).
        /// </summary>
        public LingerOption LingerOption { get; set; }
    }
}
