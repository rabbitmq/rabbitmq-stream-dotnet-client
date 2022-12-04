// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Diagnostics;
using System.Reflection;

namespace RabbitMQ.Stream.Client
{
    public static class Version
    {
        private static readonly string _versionString;

        static Version()
        {
            var a = Assembly.GetAssembly(typeof(Version));
            var fvi = FileVersionInfo.GetVersionInfo(a.Location);
            _versionString = fvi.ProductVersion;
        }

        public static string VersionString => _versionString;
    }
}
