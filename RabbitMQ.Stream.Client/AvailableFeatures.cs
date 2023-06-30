// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2023 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace RabbitMQ.Stream.Client;

/// <summary>
/// FeaturesEnabled holds the features enabled by the server and the client.
/// </summary>
internal class AvailableFeatures
{
    public bool PublishFilter { get; private set; }

    public bool Is311OrMore { get; private set; }

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
        var v = ExtractVersion(brokerVersion);
        Is311OrMore = new System.Version(v) >= new System.Version("3.11.0");
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
