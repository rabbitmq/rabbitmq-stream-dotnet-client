using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Channels;
using System.Text;
using System.Linq;
using System.Threading;

namespace RabbitMQ.Stream.Client
{
    class Program
    {
        static IDictionary<uint, TaskCompletionSource<byte>> requests = new ConcurrentDictionary<uint, TaskCompletionSource<byte>>();

        static async Task Main(string[] args)
        {
            var clientParameters = new ClientParameters{};
            var client = await Client.Create(clientParameters);
            int numConfirmed = 0;

            Action<ulong[]> confirmed = (pubIds) =>
            {
                numConfirmed = numConfirmed + pubIds.Length;
            };
            Action<(ulong, ResponseCode)[]> errored = (errors) =>
            {
            };

            await client.DeclarePublisher("my-publisher", "s1", confirmed, errored);
            for (ulong i = 0; i < 10000; i++)
            {
                var msgData = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(
                    "asdfasdfasdfasdfljasdlfjasdlkfjalsdkfjlasdkjfalsdkjflaskdjflasdjkflkasdjflasdjflaksdjflsakdjflsakdjflasdkjflasdjflaksfdhi"));
                client.Publish(new OutgoingMsg(0, i, msgData));
                if((int)i - numConfirmed > 1000)
                    await Task.Delay(10);
            }
            await Task.Delay(1000);
            Console.WriteLine($"num confirmed {numConfirmed}");

            var closeResponse = await client.Close("finished");
            await Task.Delay(2000);

            // var _subResult = await client.Write(new SubscribeRequest(10, 0, "s1", new OffsetTypeFirst(), 3, new Dictionary<string, string>()));
            // while (true)
            // {
            //     var cmd = await channel.Reader.ReadAsync();
            //     Console.WriteLine($"command {cmd}");
            //     if (cmd is PublishConfirm pc)
            //     {
            //         Console.WriteLine($"confirm: {pc.PublisherId} {pc.PublishingIds.Length}");
            //     }
            //     if (cmd is Deliver d)
            //     {
            //         Console.WriteLine($"deliver: {d.Messages.Count()} ");
            //     }
            // }
        }
    }

    public interface IResponse
    {
        static ushort ResponseKey { get; }
        int Read(ReadOnlySequence<byte> frame, out ICommand command);
    }

    public class Consumer : IDisposable
    {
        public void Dispose()
        {
        }
    }
}
