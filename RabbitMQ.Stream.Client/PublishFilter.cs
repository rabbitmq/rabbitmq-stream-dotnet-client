// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public readonly struct PublishFilter : ICommand
    {
        private const ushort Key = 2;
        private readonly Func<Message, string> _filterValueExtractor;
        private static byte Version => Consts.Version2;

        public int SizeNeeded
        {
            get
            {
                var size = 9; // pre amble 
                foreach (var (_, msg) in messages)
                {
                    int additionalSize;
                    if (_filterValueExtractor?.Invoke(msg) is { } filterValue)
                    {
                        additionalSize = WireFormatting.StringSize(filterValue);
                    }
                    else
                    {
                        additionalSize = sizeof(short);
                    }

                    size += 8 + 4 + msg.Size + additionalSize;
                }

                return size;
            }
        }

        private readonly byte publisherId;
        private readonly List<(ulong, Message)> messages;
        private int MessageCount { get; }

        public PublishFilter(byte publisherId, List<(ulong, Message)> messages,
            Func<Message, string> filterValueExtractor)
        {
            this.publisherId = publisherId;
            this.messages = messages;
            _filterValueExtractor = filterValueExtractor;
            MessageCount = messages.Count;
        }

        public int Write(Span<byte> span)
        {
            var offset = WireFormatting.WriteUInt16(span, Key);
            offset += WireFormatting.WriteUInt16(span[offset..], Version);
            offset += WireFormatting.WriteByte(span[offset..], publisherId);
            // this assumes we never write an empty publish frame
            offset += WireFormatting.WriteInt32(span[offset..], MessageCount);
            foreach (var (publishingId, msg) in messages)
            {
                

                offset += WireFormatting.WriteUInt64(span[offset..], publishingId);
                
                if (_filterValueExtractor?.Invoke(msg) is { } filterValue)
                {
                    offset += WireFormatting.WriteString(span[offset..], filterValue);
                }
                else
                {
                    offset += WireFormatting.WriteInt16(span[offset..], -1);
                }
                
                // this only write "simple" messages, we assume msg is just the binary body
                // not stream encoded data
                offset += WireFormatting.WriteUInt32(span[offset..], (uint)msg.Size);
                offset += msg.Write(span[offset..]);
            }

            return offset;
        }
    }
}
