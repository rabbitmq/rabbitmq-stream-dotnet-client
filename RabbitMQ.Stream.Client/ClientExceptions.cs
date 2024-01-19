// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Linq;
using System.Net.Sockets;

namespace RabbitMQ.Stream.Client
{
    internal static class ClientExceptions
    {
        // <summary>
        /// IsAKnownException returns true if the exception is a known exception
        /// We need it to reconnect when the producer/consumer.
        /// - LeaderNotFoundException is a temporary exception
        ///   It means that the leader is not available and the client can't reconnect.
        ///   Especially the Producer that needs to know the leader.
        /// - SocketException
        ///   Client is trying to connect in a not ready endpoint.
        ///   It is usually a temporary situation.
        /// -  TimeoutException
        ///    Network call timed out. It is often a temporary situation and we should retry.
        ///    In this case we can try to reconnect.
        ///
        ///  For the other kind of exception, we just throw back the exception.
        //</summary>
        internal static bool IsAKnownException(Exception exception)
        {
            if (exception is AggregateException aggregateException)
            {
                var x = aggregateException.InnerExceptions.Select(x =>
                    x.GetType() == typeof(SocketException) ||
                    x.GetType() == typeof(TimeoutException) ||
                    x.GetType() == typeof(LeaderNotFoundException) ||
                    x.GetType() == typeof(OperationCanceledException) ||
                    x.GetType() == typeof(InvalidOperationException));
                return x.Any();
            }

            return exception is (SocketException or TimeoutException or LeaderNotFoundException or InvalidOperationException or OperationCanceledException) ||
                   IsStreamNotAvailable(exception);
        }

        internal static bool IsStreamNotAvailable(Exception exception)
        {
            // StreamNotAvailable is a temporary exception it can happen when the stream is just created and
            // it is not ready yet to all the nodes. In this case we can try to reconnect.
            return exception is CreateException { ResponseCode: ResponseCode.StreamNotAvailable };
        }

        internal static void CheckLeader(StreamInfo metaStreamInfo)
        {
            if (metaStreamInfo.Leader.Equals(default(Broker)))
            {
                throw new LeaderNotFoundException(
                    $"No leader found for streams {string.Join(" ", metaStreamInfo.Stream)}");
            }
        }

        public static void MaybeThrowException(ResponseCode responseCode, string message)
        {
            if (responseCode is ResponseCode.Ok)
            {
                return;
            }

            throw responseCode switch
            {
                ResponseCode.VirtualHostAccessFailure => new VirtualHostAccessFailureException(message),
                ResponseCode.SaslAuthenticationFailureLoopback => new AuthenticationFailureLoopback(message),
                ResponseCode.AuthenticationFailure => new AuthenticationFailureException(message),
                ResponseCode.OffsetNotFound => new OffsetNotFoundException(message),
                //TODO Implement for all the response code
                _ => new GenericProtocolException(responseCode, message)
            };
        }
    }

    public class AlreadyClosedException : Exception
    {
        public AlreadyClosedException(string s)
            : base(s)
        {
        }
    }

    public class ProtocolException : Exception
    {
        protected ProtocolException(string s)
            : base(s)
        {
        }
    }

    public class LeaderNotFoundException : ProtocolException
    {
        public LeaderNotFoundException(string s)
            : base(s)
        {
        }
    }

    public class GenericProtocolException : ProtocolException
    {
        public GenericProtocolException(ResponseCode responseCode, string s)
            : base($"response code: {responseCode} - {s}")
        {
        }
    }

    public class AuthenticationFailureException : ProtocolException
    {
        public AuthenticationFailureException(string s)
            : base(s)
        {
        }
    }

    public class AuthenticationFailureLoopback : ProtocolException
    {
        public AuthenticationFailureLoopback(string s)
            : base(s)
        {
        }
    }

    public class VirtualHostAccessFailureException : ProtocolException
    {
        public VirtualHostAccessFailureException(string s)
            : base(s)
        {
        }
    }

    public class OffsetNotFoundException : ProtocolException
    {
        public OffsetNotFoundException(string s)
            : base(s)
        {
        }
    }

    // RouteNotFoundException the exception for super stream publish
    // RouteNotFoundException is raised when the message can't be routed to any stream.
    // In this case the user will receive a timeout error and this exception is raised
    public class RouteNotFoundException : ProtocolException
    {
        public RouteNotFoundException(string s)
            : base(s)
        {
        }
    }

    public class AuthMechanismNotSupportedException : Exception
    {
        public AuthMechanismNotSupportedException(string s)
            : base(s)
        {
        }
    }

    public class UnsupportedOperationException : Exception
    {
        public UnsupportedOperationException(string s)
            : base(s)
        {
        }
    }

    public class UnknownCommandException : Exception
    {
        public UnknownCommandException(string s)
            : base(s)
        {
        }
    }

    public class CrcException : Exception
    {
        public CrcException(string s)
            : base(s)
        {
        }
    }

    public class TooManyConnectionsException : Exception
    {
        public TooManyConnectionsException(string s)
            : base(s)
        {
        }
    }
}
