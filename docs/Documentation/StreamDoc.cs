namespace Documentation;

public class StreamExample
{
    public static void Main(string[] args)
    {
        Console.WriteLine("Stream Client Documentation");
        switch (args[0])
        {
            case "--gs":
                GettingStarted.Start().Wait();
                break;
            default:
                Console.WriteLine("Unknown example");
                break;
        }

        Console.ReadLine();

        Console.WriteLine("End...");
    }
}
