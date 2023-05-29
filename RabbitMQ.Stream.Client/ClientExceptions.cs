// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;

namespace RabbitMQ.Stream.Client
{
    internal static class ClientExceptions
    {
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

    // This is the only exception is for super stream publish
    // All the other exceptions are handled internally
    // In case of routingType is Key the routing could be not valid and the message is not sent
    // to any stream. In this case the user will receive a timeout error and this exception is raised
    public class RouteNotFoundException : ProtocolException
    {
        public RouteNotFoundException(string s)
            : base(s)
        {
        }
    }
}
