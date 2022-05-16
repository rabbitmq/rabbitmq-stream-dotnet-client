// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests
{
    public class HttpClientTests
    {
        private class Connection
        {
            public string name { get; set; }
            public Dictionary<string, string> client_properties { get; set; }
        }

        private readonly ITestOutputHelper testOutputHelper;

        public HttpClientTests(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async void TestGetConnections()
        {
            using var handler = new HttpClientHandler { Credentials = new NetworkCredential("guest", "guest"), };
            using var client = new HttpClient(handler);

            var result = await client.GetAsync("http://localhost:15672/api/connections");
            if (!result.IsSuccessStatusCode)
            {
                throw new XunitException(string.Format("HTTP GET failed: {0} {1}", result.StatusCode, result.ReasonPhrase));
            }

            string body = await result.Content.ReadAsStringAsync();
            testOutputHelper.WriteLine("CONNECTIONS: {0}", body);
        }
    }
}
