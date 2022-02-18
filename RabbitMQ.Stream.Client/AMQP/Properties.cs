// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client.AMQP
{
    public class Properties : IWritable
    {
        public object MessageId { get; set; }
        public byte[] UserId { get; set; }
        public string To { get; set; }
        public string Subject { get; set; }
        public string ReplyTo { get; set; }
        public object CorrelationId { get; set; }
        public string ContentType { get; set; }
        public string ContentEncoding { get; set; }
        public DateTime AbsoluteExpiryTime { get; set; }
        public DateTime CreationTime { get; set; }
        public string GroupId { get; set; }
        public uint GroupSequence { get; set; }
        public string ReplyToGroupId { get; set; }

        public static Properties Parse(ReadOnlySequence<byte> amqpData, ref int byteRead)
        {
            var offset = AmqpWireFormatting.ReadCompositeHeader(amqpData, out var fields, out _);
            //TODO WIRE check the next
            var p = new Properties();
            for (var index = 0; index < fields; index++)
            {
                offset += AmqpWireFormatting.TryReadNull(amqpData.Slice(offset), out var value);

                if (!value)
                {
                    switch (index)
                    {
                        case 0:
                            offset += AmqpWireFormatting.ReadAny(amqpData.Slice(offset), out var messageId);
                            p.MessageId = messageId;
                            break;
                        case 1:
                            offset += AmqpWireFormatting.ReadBinary(amqpData.Slice(offset), out var userId);
                            p.UserId = userId;
                            break;
                        case 2:
                            offset += AmqpWireFormatting.ReadString(amqpData.Slice(offset), out var to);
                            p.To = to;
                            break;
                        case 3:
                            offset += AmqpWireFormatting.ReadString(amqpData.Slice(offset), out var subject);
                            p.Subject = subject;
                            break;
                        case 4:
                            offset += AmqpWireFormatting.ReadString(amqpData.Slice(offset), out var replyTo);
                            p.ReplyTo = replyTo;
                            break;
                        case 5:
                            offset += AmqpWireFormatting.ReadAny(amqpData.Slice(offset), out var correlationId);
                            p.CorrelationId = correlationId;
                            break;
                        case 6:
                            offset += AmqpWireFormatting.ReadString(amqpData.Slice(offset), out var contentType);
                            p.ContentType = contentType;
                            break;
                        case 7:
                            offset += AmqpWireFormatting.ReadString(amqpData.Slice(offset), out var contentEncoding);
                            p.ContentEncoding = contentEncoding;
                            break;
                        case 8:
                            offset += AmqpWireFormatting.ReadTimestamp(amqpData.Slice(offset),
                                out var absoluteExpiryTime);
                            p.AbsoluteExpiryTime = absoluteExpiryTime;
                            break;
                        case 9:
                            offset += AmqpWireFormatting.ReadTimestamp(amqpData.Slice(offset), out var creationTime);
                            p.CreationTime = creationTime;
                            break;
                        case 10:
                            offset += AmqpWireFormatting.ReadString(amqpData.Slice(offset), out var groupId);
                            p.GroupId = groupId;
                            break;
                        case 11:
                            offset += AmqpWireFormatting.ReadUint32(amqpData.Slice(offset), out var groupSequence);
                            p.GroupSequence = groupSequence;
                            break;
                        case 12:
                            offset += AmqpWireFormatting.ReadString(amqpData.Slice(offset), out var replyToGroupId);
                            p.ReplyToGroupId = replyToGroupId;
                            break;
                        default:
                            throw new AMQP.AmqpParseException($"Properties Parse invalid index {index}");
                    }
                }
            }

            byteRead += offset;
            return p;
        }

        private int PropertySize()
        {
            var size = AmqpWireFormatting.GetAnySize(MessageId);
            size += AmqpWireFormatting.GetAnySize(UserId);
            size += AmqpWireFormatting.GetAnySize(To);
            size += AmqpWireFormatting.GetAnySize(Subject);
            size += AmqpWireFormatting.GetAnySize(ReplyTo);
            size += AmqpWireFormatting.GetAnySize(CorrelationId);
            size += AmqpWireFormatting.GetAnySize(ContentType);
            size += AmqpWireFormatting.GetAnySize(ContentEncoding);
            size += AmqpWireFormatting.GetAnySize(AbsoluteExpiryTime);
            size += AmqpWireFormatting.GetAnySize(CreationTime);
            size += AmqpWireFormatting.GetAnySize(GroupId);
            size += AmqpWireFormatting.GetAnySize(GroupSequence);
            size += AmqpWireFormatting.GetAnySize(ReplyToGroupId);
            return size;
        }

        public int Size
        {
            get
            {
                var size = DescribedFormatCode.Size;
                size += sizeof(byte); //FormatCode.List32
                size += sizeof(uint); // field numbers
                size += sizeof(uint); // PropertySize
                return size + PropertySize();
            }
        }

        public int Write(Span<byte> span)
        {
            var offset = DescribedFormatCode.Write(span, DescribedFormatCode.MessageProperties);
            offset += WireFormatting.WriteByte(span.Slice(offset), FormatCode.List32);
            offset += WireFormatting.WriteUInt32(span.Slice(offset), (uint)PropertySize()); // PropertySize 
            offset += WireFormatting.WriteUInt32(span.Slice(offset), 13); // field numbers
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), MessageId);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), UserId);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), To);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), Subject);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), ReplyTo);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), CorrelationId);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), ContentType);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), ContentEncoding);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), AbsoluteExpiryTime);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), CreationTime);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), GroupId);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), GroupSequence);
            offset += AmqpWireFormatting.WriteAny(span.Slice(offset), ReplyToGroupId);
            return offset;
        }
    }
}
