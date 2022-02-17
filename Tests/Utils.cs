using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.AMQP;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
    public class WaitTestBeforeAfter : BeforeAfterTestAttribute
    {
        public override void Before(MethodInfo methodUnderTest)
        {
            Thread.Sleep(200);
        }

        public override void After(MethodInfo methodUnderTest)
        {
            Thread.Sleep(200);
        }
    }

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
            Thread.Sleep(300);
        }

        public static async Task PublishMessages(StreamSystem system, string stream, int numberOfMessages,
            ITestOutputHelper testOutputHelper)
        {
            var testPassed = new TaskCompletionSource<int>();
            var count = 0;
            var producer = await system.CreateProducer(
                new ProducerConfig
                {
                    Reference = "producer",
                    Stream = stream,
                    ConfirmHandler = confirmation =>
                    {
                        count++;
                        testOutputHelper.WriteLine($"Published and Confirmed: {count} messages");
                        if (count != numberOfMessages) return;
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

        public static void HttpPost(string jsonBody, string api)
        {
            var httpWebRequest = (HttpWebRequest) WebRequest.Create($"http://localhost:15672/api/{api}");
            httpWebRequest.Credentials = new System.Net.NetworkCredential("guest", "guest");
            httpWebRequest.ContentType = "application/json";
            httpWebRequest.Method = "POST";

            using (var streamWriter = new StreamWriter(httpWebRequest.GetRequestStream()))
            {
                streamWriter.Write(jsonBody);
            }

            var httpResponse = (HttpWebResponse) httpWebRequest.GetResponse();
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
                Task<byte[]> fileTask = File.ReadAllBytesAsync(filename);
                fileTask.Wait(1000);
                return fileTask.Result;
            }

            return null;
        }
    }
}