// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

namespace Filter;

public class Start
{
    private static async Task Main(string[] arguments)
    {
        if (arguments.Length == 0)
        {
            Console.WriteLine("Unknown command (values: --producer / --consumer)");
            return;
        }

        const string SteamName = "USA-States";
        switch (arguments[0])
        {
            case "--producer":
                await FilterProducer.Start(SteamName).ConfigureAwait(false);
                await Task.Delay(1).ConfigureAwait(false);
                break;
            case "--consumer":
                await FilterConsumer.Start(SteamName).ConfigureAwait(false);
                break;
            default:
                Console.WriteLine("Unknown command: {0} (values: --producer / --consumer)", arguments[0]);
                break;
        }


    }
}
