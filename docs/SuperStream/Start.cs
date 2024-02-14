﻿// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace SuperStream;

public class Start
{
    private static async Task Main(string[] arguments)
    {
        if (arguments.Length == 0)
        {
            Console.WriteLine("Unknown command (values: --producer / --consumer)");
            return;
        }

        switch (arguments[0])
        {
            case "--producer":
                await SuperStreamProducer.Start().ConfigureAwait(false);
                break;
            case "--producer-key":
                await SuperStreamProducerKey.Start().ConfigureAwait(false);
                break;
            case "--consumer":
                if (arguments.Length == 1)
                {
                    Console.WriteLine("Missing Consumer name");
                    return;
                }
                await SuperStreamConsumer.Start(arguments[1]).ConfigureAwait(false);
                break;
            default:
                Console.WriteLine("Unknown command: {0} (values: --producer / --consumer)", arguments[0]);
                break;
        }


        Console.ReadKey();
    }
}
