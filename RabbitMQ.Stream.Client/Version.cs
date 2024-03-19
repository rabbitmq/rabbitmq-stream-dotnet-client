// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Reflection;

namespace RabbitMQ.Stream.Client
{
    public static class Version
    {
        static Version()
        {
            var attr = typeof(Version).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>();
            VersionString = attr?.InformationalVersion;
        }

        public static string VersionString { get; }
    }
}
