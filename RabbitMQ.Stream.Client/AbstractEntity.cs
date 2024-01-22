// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace RabbitMQ.Stream.Client
{
    public abstract record EntityCommonConfig
    {
        internal ConnectionsPool Pool { get; set; }
        public Func<MetaDataUpdate, Task> MetadataHandler { get; set; }

        public string Identifier { get; set; }
    }

    internal enum EntityStatus
    {
        Open,
        Closed,
        Disposed,
        Initializing
    }

    public interface IClosable
    {
        public Task<ResponseCode> Close();
    }

    public abstract class AbstractEntity : IClosable
    {
        private readonly CancellationTokenSource _cancelTokenSource = new();
        protected CancellationToken Token => _cancelTokenSource.Token;
        protected ILogger Logger { get; init; }
        internal EntityStatus _status = EntityStatus.Closed;

        protected byte EntityId { get; set; }
        protected abstract string GetStream();
        protected abstract string DumpEntityConfiguration();

        protected void ThrowIfClosed()
        {
            if (!IsOpen())
            {
                throw new AlreadyClosedException($"{DumpEntityConfiguration()} is closed.");
            }
        }

        // here the _cancelTokenSource is disposed and the token is cancelled
        // in producer is used to cancel the send task
        // in consumer is used to cancel the receive task
        protected void UpdateStatusToClosed()
        {
            if (!_cancelTokenSource.IsCancellationRequested)
                _cancelTokenSource.Cancel();
            _status = EntityStatus.Closed;
        }

        public abstract Task<ResponseCode> Close();

        /// <summary>
        /// Remove the producer or consumer from the server
        /// </summary>
        /// <param name="ignoreIfAlreadyDeleted"> In case the producer or consumer is already removed from the server.
        /// ex: metadata update </param>
        /// <returns></returns>
        protected abstract Task<ResponseCode> DeleteEntityFromTheServer(bool ignoreIfAlreadyDeleted = false);

        // private readonly SemaphoreSlim _writeLock = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Internal close method. It is called by the public Close method.
        /// Set the status to closed and remove the producer or consumer from the server ( if it is not already removed )
        /// Close the TCP connection if it is not already closed or it is needed.
        /// </summary>
        /// <param name="config">The connection pool instance</param>
        /// <param name="ignoreIfAlreadyDeleted"></param>
        /// <returns></returns>
        protected async Task<ResponseCode> Shutdown(EntityCommonConfig config, bool ignoreIfAlreadyDeleted = false)
        {
            if (!IsOpen()) // the client is already closed
            {
                return ResponseCode.Ok;
            }

            UpdateStatusToClosed();
            var result = await DeleteEntityFromTheServer(ignoreIfAlreadyDeleted).ConfigureAwait(false);

            if (_client is { IsClosed: true })
            {
                return result;
            }

            var closed = await _client.MaybeClose($"closing: {EntityId}", config.Pool)
                .ConfigureAwait(false);
            ClientExceptions.MaybeThrowException(closed.ResponseCode, $"_client-close-Entity: {EntityId}");
            Logger.LogDebug("{EntityInfo} is closed", DumpEntityConfiguration());
            return result;
        }

        protected void Dispose(bool disposing)
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
                    Logger?.LogWarning("Failed to close {EntityInfo} in time", DumpEntityConfiguration());
                }
            }
            catch (Exception e)
            {
                Logger?.LogWarning("Failed to close {EntityInfo}, error {Error} ", DumpEntityConfiguration(),
                    e.Message);
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
