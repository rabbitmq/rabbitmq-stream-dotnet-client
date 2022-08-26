// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.Stream.Client
{
    public readonly struct CreditResponse : ICommand
    {
        public const ushort Key = 9;
        private readonly byte subscriptionId;
        private readonly ResponseCode responseCode;

        private CreditResponse(ResponseCode responseCode, byte subscriptionId)
        {
            this.subscriptionId = subscriptionId;
            this.responseCode = responseCode;
        }

        public int SizeNeeded => throw new NotImplementedException();

        public byte SubscriptionId => subscriptionId;

        public ResponseCode ResponseCode => responseCode;

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

        internal void HandleUnroutableCredit(IDictionary<byte, Func<Deliver, Task>> consumers)
        {
            /* the server sent a credit-response only in case of 
             * problem, e.g. crediting an unknown subscription
             * (which can happen when a consumer is closed at 
             * the same time as the deliverhandler is working
             */

            if(consumers.ContainsKey(SubscriptionId) && ResponseCode == ResponseCode.SubscriptionIdDoesNotExist)
            {
                LogEventSource.Log.LogWarning($"Consumer subscription has been unregistered " +
                    $"on the server whlie client was publishing credit command(s)");
            }
            else
            {
                throw new Exception($"An unexpected {nameof(CreditResponse)} for subscriptionId " +
                    $"{SubscriptionId} was received with responsecode {ResponseCode}");
            }
        }
    }
}
