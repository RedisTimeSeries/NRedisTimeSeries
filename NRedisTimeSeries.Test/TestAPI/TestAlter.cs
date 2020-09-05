using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestAlter : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string _key = "ALTER_TESTS";

        public TestAlter(RedisFixture redisFixture): base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(_key);
        }

        [Fact]
        public void TestAlterRetentionTime()
        {
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(_key);
            Assert.True(db.TimeSeriesAlter(_key, retentionTime: retentionTime));
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestAlterLabels()
        {
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(_key);
            Assert.True(db.TimeSeriesAlter(_key, labels: labels));
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(labels, info.Labels);
            labels.Clear();
            Assert.True(db.TimeSeriesAlter(_key, labels: labels));
            info = db.TimeSeriesInfo(_key);
            Assert.Equal(labels, info.Labels);
        }

    }
}
