using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public interface ICommand
    {
        ushort Version { get => 1; }
        uint CorrelationId => uint.MaxValue;
        public int SizeNeeded { get; }
        int Write(Span<byte> span);
    }
}
