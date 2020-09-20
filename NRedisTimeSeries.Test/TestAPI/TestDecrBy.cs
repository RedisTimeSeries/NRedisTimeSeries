using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestDecrBy : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string _key = "DECRBY_TESTS";

        public TestDecrBy(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(_key);
        }

        [Fact]
        public void TestDefaultDecrBy()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            var timeStamp = db.TimeSeriesDecrBy(_key, -value);
            Assert.True(timeStamp.UnixMilliseconds > 0);
            Assert.Equal(value, db.TimeSeriesGet(_key).Val);
        }

        [Fact]
        public void TestDecrByTimeStamp()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            TsTimeStamp timeStamp = DateTime.UtcNow;
            var result = db.TimeSeriesDecrBy(_key, -value, timeStamp);
            Assert.Equal(timeStamp, result);
            Assert.Equal(new TimeSeriesTuple(timeStamp, value), db.TimeSeriesGet(_key));
        }

        [Fact]
        public void TestDefaultDecrByWithRetentionTime()
        {
            double value = 5.5;
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            var timeStamp = db.TimeSeriesDecrBy(_key, -value, retentionTime: retentionTime);
            Assert.True(timeStamp.UnixMilliseconds > 0);
            Assert.Equal(value, db.TimeSeriesGet(_key).Val);
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestDefaultDecrByWithLabels()
        {
            double value = 5.5;
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            IDatabase db = redisFixture.Redis.GetDatabase();
            var labels = new List<TimeSeriesLabel> { label };
            var timeStamp = db.TimeSeriesDecrBy(_key, -value, labels: labels);
            Assert.True(timeStamp.UnixMilliseconds > 0);
            Assert.Equal(value, db.TimeSeriesGet(_key).Val);
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestDefaultDecrByWithUncompressed()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            var timeStamp = db.TimeSeriesDecrBy(_key, -value, uncompressed: true);
            Assert.True(timeStamp.UnixMilliseconds > 0);
            Assert.Equal(value, db.TimeSeriesGet(_key).Val);
        }
    }
}
