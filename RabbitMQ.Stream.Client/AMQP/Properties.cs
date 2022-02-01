using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace RabbitMQ.Stream.Client.AMQP
{
    public struct Properties
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

        public static Properties Parse(ReadOnlySequence<byte> amqpData, out int offset)
        {
            offset = AmqpWireFormatting.ReadCompositeHeader(amqpData, out var fields, out var next);
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

            return p;
        }
    }
}