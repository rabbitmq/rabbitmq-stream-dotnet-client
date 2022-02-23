// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2007-2020 VMware, Inc.

using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;

using RabbitMQ.Stream.Client;

using Xunit;

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

    public class EventSourceTests
    {
        /// <summary>
        /// The name of the argument whose value is used as the payload when writing the event.
        /// </summary>
        private const string ExpectedPayloadName = "message";

        /// <summary>
        /// Verifies the contents of the event
        /// object: event level match, payload
        /// name match, payload match.
        /// </summary>
        [Fact]
        public void GenerateInfoEvent()
        {
            Exception resultException = default;

            new LogEventListener().EventWritten += (sender, args) =>
            {
                resultException = Record.Exception(() => args.ValidateEventData(EventLevel.Informational, ExpectedPayloadName, new string[] { nameof(GenerateInfoEvent) }));
            };

            // Simple message.
            LogEventSource.Log.LogInformation(nameof(GenerateInfoEvent));

            // Simple message formatted with string.Format().
            LogEventSource.Log.LogInformation("{0}{1}{2}", "Generate", "Info", "Event");

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

            new LogEventListener().EventWritten += (sender, args) =>
            {
                resultException = Record.Exception(() => args.ValidateEventData(EventLevel.Warning, ExpectedPayloadName, new string[] { nameof(GenerateWarningEvent) }));
            };

            // Simple message.
            LogEventSource.Log.LogWarning(nameof(GenerateWarningEvent));

            // Simple message formatted with string.Format().
            LogEventSource.Log.LogWarning("{0}{1}{2}", "Generate", "Warning", "Event");

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

            new LogEventListener().EventWritten += (sender, args) =>
            {
                resultException = Record.Exception(() => args.ValidateEventData(EventLevel.Error, ExpectedPayloadName, new string[] { nameof(GenerateErrorEvent) }));
            };

            // Simple message.
            LogEventSource.Log.LogError(nameof(GenerateErrorEvent));

            // Simple message with null exception.
            LogEventSource.Log.LogError(nameof(GenerateErrorEvent), default);

            // Simple message formatted with string.Format().
            LogEventSource.Log.LogError("{0}{1}{2}", default, "Generate", "Error", "Event");

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
            const string Exception1Message = "TextExceptionMessage1";
            const string Exception2Message = "TextExceptionMessage2";

            Exception resultException = default;

            var exception =
                new Exception(Exception2Message,
                new Exception(Exception1Message));

            new LogEventListener().EventWritten += (sender, args) =>
            {
                resultException = Record.Exception(() => args.ValidateEventData(EventLevel.Error, ExpectedPayloadName, new string[] { nameof(GenerateErrorEvent), Exception1Message, Exception2Message }));
            };

            LogEventSource.Log.LogError(nameof(GenerateErrorEvent), exception);

            Assert.Null(resultException);
        }
    }
}
