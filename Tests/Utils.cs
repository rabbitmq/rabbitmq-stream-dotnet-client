// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class Utils<TResult>
    {
        private readonly ITestOutputHelper testOutputHelper;

        public Utils(ITestOutputHelper testOutputHelper)
        {
            this.testOutputHelper = testOutputHelper;
        }

        // Added this generic function to trap error during the 
        // tests
        public void WaitUntilTaskCompletes(TaskCompletionSource<TResult> tasks,
            bool expectToComplete = true,
            int timeOut = 10000)
        {
            try
            {
                var resultTestWait = tasks.Task.Wait(timeOut);
                Assert.Equal(resultTestWait, expectToComplete);
            }
            catch (Exception e)
            {
                testOutputHelper.WriteLine($"wait until task completes error #{e}");
                throw;
            }
        }
    }

    public static class SystemUtils
    {
        public static void Wait()
        {
            Thread.Sleep(TimeSpan.FromMilliseconds(500));
        }

        public static void Wait(TimeSpan wait)
        {
            Thread.Sleep(wait);
        }

        public static async Task PublishMessages(StreamSystem system, string stream, int numberOfMessages,
            ITestOutputHelper testOutputHelper)
        {
            await PublishMessages(system, stream, numberOfMessages, "producer", testOutputHelper);
        }

        public static async Task PublishMessages(StreamSystem system, string stream, int numberOfMessages,
            string producerName, ITestOutputHelper testOutputHelper)
        {
            var testPassed = new TaskCompletionSource<int>();
            var count = 0;
            var producer = await system.CreateProducer(
                new ProducerConfig
                {
                    Reference = producerName,
                    Stream = stream,
                    ConfirmHandler = confirmation =>
                    {
                        count++;
                        testOutputHelper.WriteLine($"Published and Confirmed: {count} messages");
                        if (count != numberOfMessages)
                        {
                            return;
                        }

                        testPassed.SetResult(count);
                    }
                });

            for (var i = 0; i < numberOfMessages; i++)
            {
                var msgData = new Data($"message_{i}".AsReadonlySequence());
                var message = new Message(msgData);
                await producer.Send(Convert.ToUInt64(i), message);
            }

            testPassed.Task.Wait(10000);
            Assert.Equal(producer.MessagesSent, numberOfMessages);
            Assert.True(producer.ConfirmFrames >= 1);
            Assert.True(producer.IncomingFrames >= 1);
            Assert.True(producer.PublishCommandsSent >= 1);
            producer.Dispose();
        }

        private class Connecction
        {
            public string name { get; set; }
            public Dictionary<string, string> client_properties { get; set; }
        }

        public static async Task<int> HttpKillConnections()
        {
            using var handler = new HttpClientHandler { Credentials = new NetworkCredential("guest", "guest"), };
            using var client = new HttpClient(handler);
            var result = await client.GetAsync("http://localhost:15672/api/connections");
            var json = await result.Content.ReadAsStringAsync();
            var connections = JsonSerializer.Deserialize<IEnumerable<Connecction>>(json);
            if (connections == null)
            {
                return 0;
            }
            // we kill _only_ producer and consumer connections
            // leave the locator up and running to delete the stream

            var iEnumerable = connections.Where(x => x.client_properties["connection_name"].Contains("to_kill"));
            foreach (var conn in iEnumerable)
            {
                await client.DeleteAsync($"http://localhost:15672/api/connections/{conn.name}");
            }

            return iEnumerable.Count();
        }

        public static void HttpPost(string jsonBody, string api)
        {
            var httpWebRequest = (HttpWebRequest)WebRequest.Create($"http://localhost:15672/api/{api}");
            httpWebRequest.Credentials = new System.Net.NetworkCredential("guest", "guest");
            httpWebRequest.ContentType = "application/json";
            httpWebRequest.Method = "POST";

            using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
            {
                streamWriter.Write(jsonBody);
            }

            var httpResponse = (HttpWebResponse)httpWebRequest.GetResponse();
            using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
            {
                streamReader.ReadToEnd();
            }
        }

        public static byte[] GetFileContent(string fileName)
        {
            var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().Location);
            var codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
            var dirPath = Path.GetDirectoryName(codeBasePath);
            if (dirPath != null)
            {
                var filename = Path.Combine(dirPath, "Resources", fileName);
                var fileTask = File.ReadAllBytesAsync(filename);
                fileTask.Wait(1000);
                return fileTask.Result;
            }

            return null;
        }
    }
}
