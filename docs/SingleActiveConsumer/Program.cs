// See https://aka.ms/new-console-template for more information

using SingleActiveConsumer;

internal class Program
{
    static async Task Main(string[] args)
    {
        if (args.Length > 0)
        {
            switch (args[0])
            {
                case "--producer":
                    await SaCProducer.Start().ConfigureAwait(false);
                    break;
                case "--consumer":
                    await SacConsumer.Start().ConfigureAwait(false);
                    break;
                default:
                    Console.WriteLine("Unknown option, valid options: --producer / --consumer");
                    break;
            }
        }
    }
}
