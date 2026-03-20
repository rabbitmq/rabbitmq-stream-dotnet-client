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

        /// <summary>
        /// Deprecated TCP keep-alive option.
        /// </summary>
        /// <remarks>
        /// This property is intentionally ignored by the connection implementation and does not
        /// enable TCP keep-alive on the underlying socket. The client library automatically
        /// reconnects in case of connection failures, so explicit TCP keep-alive is not required.
        /// The property is retained only for backward compatibility and to avoid a breaking
        /// change in the public API; new code should not rely on it.
        /// </remarks>
        [Obsolete("KeepAlive is not needed since the library will automatically reconnect in case of connection failure.")]
        public bool KeepAlive { get; set; } = true;

        /// <summary>
        /// Linger option on close. When set, controls whether the socket waits for unsent data
        /// and for how long (seconds). When null, the system default is used (typically abort on close).
        /// </summary>
        public LingerOption LingerOption { get; set; }
    }
}
