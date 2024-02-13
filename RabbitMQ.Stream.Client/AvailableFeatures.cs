// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace RabbitMQ.Stream.Client;

/// <summary>
/// AvailableFeatures holds the features enabled by the server and the client.
/// </summary>
internal class AvailableFeatures
{
    public bool PublishFilter { get; private set; }

    public bool Is311OrMore { get; private set; }
    public bool Is313OrMore { get; private set; }

    public string BrokerVersion { get; private set; }

    private static string ExtractVersion(string fullVersion)
    {
        const string Pattern = @"(\d+\.\d+\.\d+)";
        var match = Regex.Match(fullVersion, Pattern);

        return match.Success
            ? match.Groups[1].Value
            : string.Empty;
    }

    public void SetServerVersion(string brokerVersion)
    {
        BrokerVersion = ExtractVersion(brokerVersion);
        Is311OrMore = new System.Version(BrokerVersion) >= new System.Version("3.11.0");
        Is313OrMore = new System.Version(BrokerVersion) >= new System.Version("3.13.0");
    }

    public void ParseCommandVersions(List<ICommandVersions> commands)
    {
        foreach (var command in commands)
        {
            switch (command.Command)
            {
                case Stream.Client.PublishFilter.Key:
                    var p = new PublishFilter();
                    PublishFilter = command.MinVersion <= p.MinVersion &&
                                    command.MaxVersion >= p.MaxVersion;
                    break;
            }
        }
    }
}

internal sealed class AvailableFeaturesSingleton
{
    private static readonly Lazy<AvailableFeatures> s_lazy =
        new(() => new AvailableFeatures());

    public static AvailableFeatures Instance => s_lazy.Value;
}
