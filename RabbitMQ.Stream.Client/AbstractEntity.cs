// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System.Threading;

namespace RabbitMQ.Stream.Client
{
    public abstract class AbstractEntity
    {
        private readonly CancellationTokenSource _cancelTokenSource = new();
        protected CancellationToken Token => _cancelTokenSource.Token;

        // here the _cancelTokenSource is disposed and the token is cancelled
        // in producer is used to cancel the send task
        // in consumer is used to cancel the receive task
        protected void MaybeCancelToken()
        {
            if (!_cancelTokenSource.IsCancellationRequested)
                _cancelTokenSource.Cancel();
        }

        protected Client _client;

        public Info Info { get; internal set; }
    }
}
