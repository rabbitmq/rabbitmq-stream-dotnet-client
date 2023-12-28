// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.Stream.Client;

public interface ICommandVersions
{
    public ushort MaxVersion { get; }
    public ushort MinVersion { get; }
    public ushort Command { get; }
}

public class CommandVersions : ICommandVersions
{
    public CommandVersions(ushort command, ushort minVersion, ushort maxVersion)
    {
        Command = command;
        MinVersion = minVersion;
        MaxVersion = maxVersion;
    }

    public ushort MaxVersion { get; }
    public ushort MinVersion { get; }
    public ushort Command { get; }
}
