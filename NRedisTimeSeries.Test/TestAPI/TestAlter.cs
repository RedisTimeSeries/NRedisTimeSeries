using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestAlter : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "ALTER_TESTS";

        public TestAlter(RedisFixture redisFixture): base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestAlterRetentionTime()
        {
            long retentionTime = 5000;
            IDatabase db = redisFixture.redis.GetDatabase();
            db.TimeSeriesCreate(key);
            Assert.True(db.TimeSeriesAlter(key, retentionTime: retentionTime));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestAlterLabels()
        {
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            IDatabase db = redisFixture.redis.GetDatabase();
            db.TimeSeriesCreate(key);
            Assert.True(db.TimeSeriesAlter(key, labels: labels));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(labels, info.Labels);
            labels.Clear();
            Assert.True(db.TimeSeriesAlter(key, labels: labels));
            info = db.TimeSeriesInfo(key);
            Assert.Equal(labels, info.Labels);
        }

    }
}
