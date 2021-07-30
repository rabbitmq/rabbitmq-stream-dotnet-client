using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using RabbitMQ.Stream.Client;
using Xunit;

namespace Tests
{
    public class UnitTest1
    {
        [Fact]
        public async void Test1()
        {
            var userName = "guest";
            var password = "guest";
            var channel = Channel.CreateUnbounded<ICommand>();
            Action<ICommand> callback = (command) =>
            {
                channel.Writer.TryWrite(command);
            };

            var client = new Connection(callback);
            var peerProps = new PeerPropertiesRequest(2, new Dictionary<string, string>{
                {"key", "value"}
            });
            var _ = await client.Write(peerProps);
            var ppResponse = await channel.Reader.ReadAsync();
            if (ppResponse is PeerPropertiesResponse serverProps)
            {
                Console.WriteLine($"server Props {serverProps.Properties}");
            }

            var _saslResult = await client.Write(new SaslHandshakeRequest(3));
            var saslResponse = await channel.Reader.ReadAsync();
            if (saslResponse is SaslHandshakeResponse sasl)
            {
                foreach (var m in sasl.Mechanisms)
                    Console.WriteLine($"sasl: {m}");
            }

            var saslData = Encoding.UTF8.GetBytes($"\0{userName}\0{password}");
            var _authResult = await client.Write(new SaslAuthenticateRequest(4, "PLAIN", saslData));
            var authResponse = await channel.Reader.ReadAsync();

            if (authResponse is SaslAuthenticateResponse auth)
            {
                Console.WriteLine($"auth: {auth.ResponseCode} {auth.Data}");
            }

            var _tuneResult = await client.Write(new TuneRequest(0, 0));
            var tune = await channel.Reader.ReadAsync();
            // open 
            var _openResult = await client.Write(new OpenRequest(5, "/"));
            var open = await channel.Reader.ReadAsync();
            if (open is OpenResponse o)
            {
                Console.WriteLine($"open: {o.ResponseCode} {o.ConnectionProperties.Count}");
                foreach (var (k, v) in o.ConnectionProperties)
                    Console.WriteLine($"open prop: {k} {v}");
            }

            var _pubResult = await client.Write(new DeclarePublisherRequest(6, 0, "pref1", "s1"));
            var declPub = await channel.Reader.ReadAsync();
            if (declPub is DeclarePublisherResponse dp)
            {
                Console.WriteLine($"declPub: {dp.ResponseCode} {dp.CorrelationId}");
            }


            var msg = new ReadOnlySequence<byte>(UTF8Encoding.UTF8.GetBytes("hi"));
            var _publishResult = await client.Write(new Publish(0, new List<(ulong, ReadOnlySequence<byte>)> { (0, msg) }));

            var _subResult = await client.Write(new SubscribeRequest(10, 0, "s1", new OffsetTypeFirst(), 3, new Dictionary<string, string>()));
            while (true)
            {
                var cmd = await channel.Reader.ReadAsync();
                Console.WriteLine($"command {cmd}");
                if (cmd is PublishConfirm pc)
                {
                    Console.WriteLine($"confirm: {pc.PublisherId} {pc.PublishingIds.Length}");
                }
                if (cmd is Deliver d)
                {
                    Console.WriteLine($"deliver: {d.Messages.Count()} ");
                }
            }
        }
    }
}
