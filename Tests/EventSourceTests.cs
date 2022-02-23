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

            LogEventSource.Log.LogInformation(nameof(GenerateInfoEvent));

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

            LogEventSource.Log.LogWarning(nameof(GenerateWarningEvent));

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

            LogEventSource.Log.LogError(nameof(GenerateErrorEvent));
            LogEventSource.Log.LogError(nameof(GenerateErrorEvent), default);

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
            const string exception1Message = "TextExceptionMessage1";
            const string exception2Message = "TextExceptionMessage2";

            var message = $"GenerateErrorEvent{ Environment.NewLine }System.Exception: { exception2Message }{ Environment.NewLine } ---> System.Exception: { exception1Message }{ Environment.NewLine }   --- End of inner exception stack trace ---";

            Exception resultException = default;

            var exception =
                new Exception(exception2Message,
                new Exception(exception1Message));

            new LogEventListener().EventWritten += (sender, args) =>
            {
                resultException = Record.Exception(() => args.ValidateEventData(EventLevel.Error, ExpectedPayloadName, new string[] { message }));
            };

            LogEventSource.Log.LogError(nameof(GenerateErrorEvent), exception);

            Assert.Null(resultException);
        }
    }
}
