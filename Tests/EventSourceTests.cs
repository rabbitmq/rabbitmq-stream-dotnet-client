// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.IO;
using System.Text;

using RabbitMQ.Stream.Client;

using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public static class EventSourceTestsHelper
    {
        public static void ValidateEventData(this EventWrittenEventArgs args, EventLevel level, string expectedPayloadName, IEnumerable<string> expectedPayloadText)
        {
            Assert.Equal(level, args.Level);

            Assert.Equal(expectedPayloadName, args.PayloadNames[0]);

            foreach (var payloadText in expectedPayloadText)
            {
                Assert.Contains(payloadText, args.Payload[0].ToString());
            }
        }
    }

    public class EventSourceTests : IDisposable
    {
        private readonly LogEventListener logEventListener;

        public EventSourceTests(ITestOutputHelper testOutputHelper)
        {
            logEventListener = new LogEventListener(new TestOutputTextWriter(testOutputHelper));
        }

        /// <summary>
        /// The name of the argument whose value is used as the payload when writing the event.
        /// </summary>
        private const string ExpectedPayloadName = "message";

        public void Dispose()
        {
            logEventListener.Dispose();
        }

        /// <summary>
        /// Verifies the contents of the event
        /// object: event level match, payload
        /// name match, payload match.
        /// </summary>
        [Fact]
        public void GenerateInfoEvent()
        {
            Exception resultException = default;

            EventHandler<EventWrittenEventArgs> handler = (sender, args) =>
            {
                resultException = Record.Exception(() =>
                        args.ValidateEventData(EventLevel.Informational, ExpectedPayloadName, new string[] { nameof(GenerateInfoEvent) }));
            };

            try
            {
                logEventListener.EventWritten += handler;

                // Simple message.
                LogEventSource.Log.LogInformation(nameof(GenerateInfoEvent));

                // Simple message formatted with string.Format().
                LogEventSource.Log.LogInformation("{0}{1}{2}", "Generate", "Info", "Event");
            }
            finally
            {
                logEventListener.EventWritten -= handler;
            }

            Assert.Null(resultException);
        }

        /// <summary>
        /// Verifies the contents of the event
        /// object: event level match, payload
        /// name match, payload match.
        /// </summary>
        [Fact]
        public void GenerateWarningEvent()
        {
            Exception resultException = default;

            EventHandler<EventWrittenEventArgs> handler = (sender, args) =>
            {
                resultException = Record.Exception(() => args.ValidateEventData(EventLevel.Warning, ExpectedPayloadName, new string[] { nameof(GenerateWarningEvent) }));
            };

            try
            {
                logEventListener.EventWritten += handler;

                // Simple message.
                LogEventSource.Log.LogWarning(nameof(GenerateWarningEvent));

                // Simple message formatted with string.Format().
                LogEventSource.Log.LogWarning("{0}{1}{2}", "Generate", "Warning", "Event");
            }
            finally
            {
                logEventListener.EventWritten -= handler;
            }

            Assert.Null(resultException);
        }

        /// <summary>
        /// Verifies the contents of the event
        /// object: event level match, payload
        /// name match, payload match.
        /// </summary>
        [Fact]
        public void GenerateErrorEvent()
        {
            Exception resultException = default;

            EventHandler<EventWrittenEventArgs> handler = (sender, args) =>
            {
                resultException = Record.Exception(() => args.ValidateEventData(EventLevel.Error, ExpectedPayloadName, new string[] { nameof(GenerateErrorEvent) }));
            };

            try
            {
                logEventListener.EventWritten += handler;

                // Simple message.
                LogEventSource.Log.LogError(nameof(GenerateErrorEvent));

                // Simple message with null exception.
                LogEventSource.Log.LogError(nameof(GenerateErrorEvent), default);

                // Simple message formatted with string.Format().
                LogEventSource.Log.LogError("{0}{1}{2}", default, "Generate", "Error", "Event");
            }
            finally
            {
                logEventListener.EventWritten -= handler;
            }

            Assert.Null(resultException);
        }

        /// <summary>
        /// Verifies the contents of the event
        /// object: event level match, payload
        /// name match, payload match.
        /// </summary>
        [Fact]
        public void GenerateErrorWithExceptionEvent()
        {
            const string Exception0Message = "TextExceptionMessage0";
            const string Exception1Message = "TextExceptionMessage1";
            const string Exception2Message = "TextExceptionMessage2";

            Exception resultException = default;

            var exception0 = new Exception(Exception0Message);
            exception0.Data.Add("ex0", "data0");

            var exception1 = new Exception(Exception1Message, exception0);
            exception1.Data.Add("foo", "bar");

            var exception2 = new Exception(Exception2Message, exception1);
            exception2.Data.Add("baz", "bat");

            EventHandler<EventWrittenEventArgs> handler = (sender, args) =>
            {
                resultException = Record.Exception(() =>
                        args.ValidateEventData(EventLevel.Error, ExpectedPayloadName,
                            new string[] { nameof(GenerateErrorWithExceptionEvent), Exception1Message, Exception2Message }));
            };

            try
            {
                logEventListener.EventWritten += handler;

                LogEventSource.Log.LogError(nameof(GenerateErrorWithExceptionEvent), exception2);
            }
            finally
            {
                logEventListener.EventWritten -= handler;
            }

            Assert.Null(resultException);
        }

        private class TestOutputTextWriter : TextWriter
        {
            private readonly ITestOutputHelper testOutputHelper;

            public TestOutputTextWriter(ITestOutputHelper testOutputHelper) : base()
            {
                this.testOutputHelper = testOutputHelper;
            }

            public override Encoding Encoding => Encoding.UTF8;

            public override void WriteLine(string format, object arg0, object arg1, object arg2)
            {
                testOutputHelper.WriteLine(string.Format(format, arg0, arg1, arg2));
            }
        }
    }
}
