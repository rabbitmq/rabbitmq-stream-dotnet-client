using System;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client
{
    public record StreamSpec
    {
        private readonly IDictionary<string, string> args = new Dictionary<string, string>();

        public StreamSpec(string name)
        {
            Name = name;
        }

        public string Name { get; init; }

        public TimeSpan MaxAge
        {
            set => Args.Add("max-age", $"{value.TotalSeconds}s");
        }

        public int MaxLengthBytes
        {
            set => Args.Add("max-length-bytes", $"{value}");
        }
        
        public LeaderLocator LeaderLocator
        {
            set => Args.Add("queue-leader-locator", $"{value.ToString()}");
        }

        public IDictionary<string, string> Args => args;
    }
}