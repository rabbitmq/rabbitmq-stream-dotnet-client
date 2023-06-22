// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace RabbitMQ.Stream.Client
{
    public readonly struct PublishFilter : ICommand, ICommandVersions
    {
        internal const ushort Key = 2;
        private readonly Func<Message, string> _filterValueExtractor;
        private static byte Version => Consts.Version2;

        public int SizeNeeded
        {
            get
            {
                var size = 9; // pre amble 
                foreach (var (_, msg) in messages)
                {
                    try
                    {
                        var filterValue = "";
                        if (IsFilterSet())
                        {
                            filterValue = _filterValueExtractor(msg);
                        }

                        var additionalSize = IsFilterSet() ? WireFormatting.StringSize(filterValue) : sizeof(short);

                        size += 8 + 4 + msg.Size + additionalSize;
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, "Error calculate size for the filter message. Message won't be sent"
                                            + "Check the filter value function");
                    }
                }

                return size;
            }
        }

        private readonly byte publisherId;
        private readonly List<(ulong, Message)> messages;
        private readonly ILogger _logger;

        private bool IsFilterSet()
        {
            return _filterValueExtractor != null;
        }

        private int MessageCount { get; }

        public PublishFilter(byte publisherId, List<(ulong, Message)> messages,
            Func<Message, string> filterValueExtractor, ILogger logger)
        {
            this.publisherId = publisherId;
            this.messages = messages;
            _filterValueExtractor = filterValueExtractor;
            _logger = logger;
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
                try
                {
                    var filterValue = "";
                    if (IsFilterSet())
                    {
                        // The try catch is mostly for the case where the filterValueExtractor
                        // throws an exception. 
                        // The user should be aware of this and make sure the function is safe
                        // but in case of fail we have to skip the message
                        filterValue = _filterValueExtractor(msg);
                    }

                    offset += WireFormatting.WriteUInt64(span[offset..], publishingId);
                    if (IsFilterSet())
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
                catch (Exception e)
                {
                    // If there is an error on _filterValueExtractor we skip the message.
                    // If there is an error on _filterValueExtractor the buffer here is still consistent.
                    // so we can skip and continue
                    _logger.LogError(e, "Error writing the filter message. Message won't be sent"
                                        + "Check the filter value function");
                }
            }

            return offset;
        }

        public ushort MaxVersion
        {
            get => Consts.Version2;
        }

        public ushort MinVersion
        {
            get => Consts.Version2;
        }

        public ushort Command
        {
            get => Key;
        }
    }
}
