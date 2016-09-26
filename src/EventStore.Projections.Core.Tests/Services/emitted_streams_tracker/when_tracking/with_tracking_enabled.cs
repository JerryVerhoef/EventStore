using EventStore.ClientAPI.Common.Utils;
using EventStore.Projections.Core.Services.Processing;
using NUnit.Framework;
using System;
using System.Threading;

namespace EventStore.Projections.Core.Tests.Services.emitted_streams_tracker.when_tracking
{
    [TestFixture]
    public class with_tracking_enabled : SpecificationWithEmittedStreamsTrackerAndDeleter
    {
        private ManualResetEvent _eventAppeared = new ManualResetEvent(false);
        private EventStore.ClientAPI.SystemData.UserCredentials _credentials;

        protected override void Given()
        {
            _credentials = new EventStore.ClientAPI.SystemData.UserCredentials("admin", "changeit");
            base.Given();
        }

        protected override void When()
        {
            _conn.SubscribeToStreamAsync(_projectionNamesBuilder.GetEmittedStreamsName(), true, (s, evnt) => {
                _eventAppeared.Set();
            }, userCredentials: _credentials);

            _emittedStreamsTracker.TrackEmittedStream(new EmittedEvent[]
            {
                new EmittedDataEvent(
                     "test_stream", Guid.NewGuid(),  "type1",  true,
                     "data",  null, CheckpointTag.FromPosition(0, 100, 50),  null, null)
            });
        }

        [Test]
        public void should_write_a_stream_tracked_event()
        {
            if(!_eventAppeared.WaitOne(TimeSpan.FromSeconds(5))) 
            {
                Assert.Fail("Timed out waiting for emitted stream event");
            }

            var result = _conn.ReadStreamEventsForwardAsync(_projectionNamesBuilder.GetEmittedStreamsName(), 0, 200, false, _credentials).Result;
            Assert.AreEqual(1, result.Events.Length);
            Assert.AreEqual("test_stream", Helper.UTF8NoBom.GetString(result.Events[0].Event.Data));
        }
    }
}
