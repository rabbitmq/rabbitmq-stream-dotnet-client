// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Buffers;

using RabbitMQ.Stream.Client.AMQP;

namespace RabbitMQ.Stream.Client
{
    public class Message
    {
        public Message(byte[] data) : this(new Data(new ReadOnlySequence<byte>(data)))
        {
        }

        public Message(Data data)
        {
            Data = data;
        }

        public Annotations Annotations { get; internal set; }

        public ApplicationProperties ApplicationProperties { get; set; }

        public Properties Properties { get; set; }

        public Data Data { get; }

        // MessageHeader and AmqpValue are only in get.
        // Just to have the compatibility with AMQP 1.0
        // In this specific case it is not needed
        public Header MessageHeader { get; internal set; }
        public object AmqpValue { get; internal set; }

        public int Size => Data.Size +
                           (Properties?.Size ?? 0) +
                           (Annotations?.Size ?? 0) +
                           (ApplicationProperties?.Size ?? 0);

        public int Write(Span<byte> span)
        {
            var offset = 0;
            if (Properties != null)
            {
                offset += Properties.Write(span.Slice(offset));
            }

            if (ApplicationProperties != null)
            {
                offset += ApplicationProperties.Write(span.Slice(offset));
            }

            if (Annotations != null)
            {
                offset += Annotations.Write(span.Slice(offset));
            }

            offset += Data.Write(span.Slice(offset));
            return offset;
        }

        public ReadOnlySequence<byte> Serialize()
        {
            //what a massive cludge
            var data = new byte[Data.Size];
            Data.Write(data);
            return new ReadOnlySequence<byte>(data);
        }

        public static Message From(ReadOnlySequence<byte> amqpData)
        {
            //                                                         Bare Message
            //                                                             |
            //                                       .---------------------+--------------------.
            //                                      |                                           |
            // +--------+-------------+-------------+------------+--------------+--------------+--------
            // | header | delivery-   | message-    | properties | application- | application- | footer |
            // |        | annotations | annotations |             | properties  | data         |        |
            // +--------+-------------+-------------+------------+--------------+--------------+--------+ 
            // Altogether a message consists of the following sections:
            // • Zero or one header.
            // • Zero or one delivery-annotations.
            // • Zero or one message-annotations.
            // • Zero or one properties.
            // • Zero or one application-properties.
            // • The body consists of either: one or more data sections, one or more amqp-sequence sections,
            // or a single amqp-value section.
            // • Zero or one footer.

            //parse AMQP encoded data
            var offset = 0;
            Annotations annotations = null;
            Header header = null;
            Data data = default;
            Properties properties = null;
            object amqpValue = null;
            ApplicationProperties applicationProperties = null;
            while (offset != amqpData.Length)
            {
                var dataCode = DescribedFormatCode.Read(amqpData.Slice(offset));
                switch (dataCode)
                {
                    case DescribedFormatCode.ApplicationData:
                        offset += DescribedFormatCode.Size;
                        data = Data.Parse(amqpData.Slice(offset), ref offset);
                        break;
                    case DescribedFormatCode.MessageAnnotations:
                        offset += DescribedFormatCode.Size;
                        annotations = Annotations.Parse<Annotations>(amqpData.Slice(offset), ref offset);
                        break;
                    case DescribedFormatCode.MessageProperties:
                        properties = Properties.Parse(amqpData.Slice(offset), ref offset);
                        break;
                    case DescribedFormatCode.ApplicationProperties:
                        offset += DescribedFormatCode.Size;
                        applicationProperties =
                            ApplicationProperties.Parse<ApplicationProperties>(amqpData.Slice(offset), ref offset);
                        break;
                    case DescribedFormatCode.MessageHeader:
                        header = Header.Parse(amqpData.Slice(offset), ref offset);
                        break;
                    case DescribedFormatCode.AmqpValue:
                        offset += DescribedFormatCode.Size;
                        offset += AmqpWireFormatting.ReadAny(amqpData.Slice(offset), out amqpValue);
                        break;
                    default:
                        LogEventSource.Log.LogError($"dataCode: {dataCode} not handled. Please open an issue.");
                        throw new ArgumentOutOfRangeException($"dataCode: {dataCode} not handled");
                }
            }

            var msg = new Message(data)
            {
                Annotations = annotations,
                Properties = properties,
                ApplicationProperties = applicationProperties,
                AmqpValue = amqpValue,
                MessageHeader = header
            };
            return msg;
        }
    }
}
