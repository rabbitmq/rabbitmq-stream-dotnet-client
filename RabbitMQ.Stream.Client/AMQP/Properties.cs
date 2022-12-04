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

        public static Properties Parse(ref SequenceReader<byte> reader, ref int byteRead)
        {
            var offset = AmqpWireFormatting.ReadCompositeHeader(ref reader, out var fields, out _);
            //TODO WIRE check the next
            var p = new Properties();
            for (var index = 0; index < fields; index++)
            {
                offset += AmqpWireFormatting.TryReadNull(ref reader, out var value);

                if (!value)
                {
                    switch (index)
                    {
                        case 0:
                            offset += AmqpWireFormatting.ReadAny(ref reader, out var messageId);
                            p.MessageId = messageId;
                            break;
                        case 1:
                            offset += AmqpWireFormatting.ReadBinary(ref reader, out var userId);
                            p.UserId = userId;
                            break;
                        case 2:
                            offset += AmqpWireFormatting.ReadString(ref reader, out var to);
                            p.To = to;
                            break;
                        case 3:
                            offset += AmqpWireFormatting.ReadString(ref reader, out var subject);
                            p.Subject = subject;
                            break;
                        case 4:
                            offset += AmqpWireFormatting.ReadString(ref reader, out var replyTo);
                            p.ReplyTo = replyTo;
                            break;
                        case 5:
                            offset += AmqpWireFormatting.ReadAny(ref reader, out var correlationId);
                            p.CorrelationId = correlationId;
                            break;
                        case 6:
                            offset += AmqpWireFormatting.ReadString(ref reader, out var contentType);
                            p.ContentType = contentType;
                            break;
                        case 7:
                            offset += AmqpWireFormatting.ReadString(ref reader, out var contentEncoding);
                            p.ContentEncoding = contentEncoding;
                            break;
                        case 8:
                            offset += AmqpWireFormatting.ReadTimestamp(ref reader,
                                out var absoluteExpiryTime);
                            p.AbsoluteExpiryTime = absoluteExpiryTime;
                            break;
                        case 9:
                            offset += AmqpWireFormatting.ReadTimestamp(ref reader, out var creationTime);
                            p.CreationTime = creationTime;
                            break;
                        case 10:
                            offset += AmqpWireFormatting.ReadString(ref reader, out var groupId);
                            p.GroupId = groupId;
                            break;
                        case 11:
                            offset += AmqpWireFormatting.ReadUint32(ref reader, out var groupSequence);
                            p.GroupSequence = groupSequence;
                            break;
                        case 12:
                            offset += AmqpWireFormatting.ReadString(ref reader, out var replyToGroupId);
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
            offset += WireFormatting.WriteByte(span[offset..], FormatCode.List32);
            offset += WireFormatting.WriteUInt32(span[offset..], (uint)PropertySize()); // PropertySize 
            offset += WireFormatting.WriteUInt32(span[offset..], 13); // field numbers
            offset += AmqpWireFormatting.WriteAny(span[offset..], MessageId);
            offset += AmqpWireFormatting.WriteAny(span[offset..], UserId);
            offset += AmqpWireFormatting.WriteAny(span[offset..], To);
            offset += AmqpWireFormatting.WriteAny(span[offset..], Subject);
            offset += AmqpWireFormatting.WriteAny(span[offset..], ReplyTo);
            offset += AmqpWireFormatting.WriteAny(span[offset..], CorrelationId);
            offset += AmqpWireFormatting.WriteAny(span[offset..], ContentType);
            offset += AmqpWireFormatting.WriteAny(span[offset..], ContentEncoding);
            offset += AmqpWireFormatting.WriteAny(span[offset..], AbsoluteExpiryTime);
            offset += AmqpWireFormatting.WriteAny(span[offset..], CreationTime);
            offset += AmqpWireFormatting.WriteAny(span[offset..], GroupId);
            offset += AmqpWireFormatting.WriteAny(span[offset..], GroupSequence);
            offset += AmqpWireFormatting.WriteAny(span[offset..], ReplyToGroupId);
            return offset;
        }
    }
}
