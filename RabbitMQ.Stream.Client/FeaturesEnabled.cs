// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Generic;

namespace RabbitMQ.Stream.Client;

/// <summary>
/// FeaturesEnabled holds the features enabled by the server and the client.
/// </summary>
internal class FeaturesEnabled
{
    public bool IsPublishFilterEnabled { get; private set; }

    public bool Is311OrMore { get; private set; }

    public void ParseServerVersion(string brokerVersion)
    {
        Is311OrMore = new System.Version(brokerVersion) >= new System.Version("3.11.0");
    }

    public void ParseCommandVersions(List<ICommandVersions> commands)
    {
        foreach (var command in commands)
        {
            switch (command.Command)
            {
                case PublishFilter.Key:
                    var p = new PublishFilter();
                    IsPublishFilterEnabled = command.MinVersion <= p.MinVersion &&
                                             command.MaxVersion >= p.MaxVersion;
                    break;
            }
        }
    }
}

internal sealed class FeaturesEnabledSingleton
{
    private static readonly Lazy<FeaturesEnabled> s_lazy =
        new(() => new FeaturesEnabled());

    public static FeaturesEnabled Instance => s_lazy.Value;
}
