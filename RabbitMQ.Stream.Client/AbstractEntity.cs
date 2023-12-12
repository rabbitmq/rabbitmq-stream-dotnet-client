// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace RabbitMQ.Stream.Client
{
    internal enum EntityStatus
    {
        Open,
        Closed,
        Disposed
    }

    public interface IClosable
    {
        public Task<ResponseCode> Close(bool ignoreIfClosed = false);
    }

    public abstract class AbstractEntity : IClosable
    {
        private readonly CancellationTokenSource _cancelTokenSource = new();
        protected CancellationToken Token => _cancelTokenSource.Token;

        internal EntityStatus _status = EntityStatus.Closed;

        // here the _cancelTokenSource is disposed and the token is cancelled
        // in producer is used to cancel the send task
        // in consumer is used to cancel the receive task
        protected void MaybeCancelToken()
        {
            if (!_cancelTokenSource.IsCancellationRequested)
                _cancelTokenSource.Cancel();
        }

        public abstract Task<ResponseCode> Close(bool ignoreIfClosed = false);

        protected void Dispose(bool disposing, string entityInfo, ILogger logger)
        {
            if (!disposing)
            {
                return;
            }

            if (_status == EntityStatus.Disposed)
            {
                return;
            }

            try
            {
                var closeTask = Close();
                if (!closeTask.Wait(Consts.MidWait))
                {
                    logger.LogWarning("Failed to close {EntityInfo} in time", entityInfo);
                }
            }
            catch (Exception e)
            {
                logger?.LogWarning("Failed to close {EntityInfo}, error {Error} ", entityInfo, e.Message);
            }
            finally
            {
                _status = EntityStatus.Disposed;
            }
        }

        public bool IsOpen()
        {
            return _status == EntityStatus.Open;
        }

        internal Client _client;
    }
}
