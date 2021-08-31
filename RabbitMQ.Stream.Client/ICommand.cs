using System;
using System.Buffers;

namespace RabbitMQ.Stream.Client
{
    public interface ICommand
    {
        ushort Version => 1;
        uint CorrelationId => uint.MaxValue;
        static ushort Key { get; }
        public int SizeNeeded { get; }
        int Write(Span<byte> span);
    }
}
