// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Threading.Tasks;
using PublicApiGenerator;
using RabbitMQ.Stream.Client;
using VerifyTests;
using VerifyXunit;
using Xunit;

namespace Tests
{
    [UsesVerify]
    public class ApiApproval
    {
        [SkippableFact]
        public Task Approve()
        {
            Skip.IfNot(OperatingSystem.IsWindows());

            var publicApi = typeof(Client).Assembly.GeneratePublicApi(new ApiGeneratorOptions
            {
                ExcludeAttributes = new[]
                {
                    "System.Runtime.Versioning.TargetFrameworkAttribute",
                    "System.Reflection.AssemblyMetadataAttribute"
                }
            });

            var settings = new VerifySettings();
            settings.DisableDiff();

            return Verifier.Verify(publicApi, settings);
        }
    }
}
