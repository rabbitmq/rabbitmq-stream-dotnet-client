// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public readonly struct CreditResponse : ICommand
    {
        public const ushort Key = 9;

        private CreditResponse(ResponseCode responseCode, byte subscriptionId)
        {
            SubscriptionId = subscriptionId;
            ResponseCode = responseCode;
        }

        public int SizeNeeded => throw new NotImplementedException();

        private byte SubscriptionId { get; }

        private ResponseCode ResponseCode { get; }

        public int Write(Span<byte> span)
        {
            throw new NotImplementedException();
        }

        internal static int Read(ReadOnlySequence<byte> frame, out CreditResponse command)
        {
            var offset = WireFormatting.ReadUInt16(frame, out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out _);
            offset += WireFormatting.ReadUInt16(frame.Slice(offset), out var responseCode);
            offset += WireFormatting.ReadByte(frame.Slice(offset), out var subscriptionId);

            command = new CreditResponse((ResponseCode)responseCode, subscriptionId);
            return offset;
        }

        internal void HandleUnRoutableCredit()
        {
            /* the server sends a credit-response only in case of 
             * problem, e.g. crediting an unknown subscription
             * (which can happen when a consumer is closed at 
             * the same time as the deliverhandler is working
             */

            LogEventSource.Log.LogWarning(
                $"Received notification for subscription id: {SubscriptionId} " +
                $"code: {ResponseCode}");
        }
    }
}
