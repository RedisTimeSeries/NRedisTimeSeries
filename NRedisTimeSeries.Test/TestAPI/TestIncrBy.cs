using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestIncrBy : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string _key = "INCRBY_TESTS";

        public TestIncrBy(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(_key);
        }

        [Fact]
        public void TestDefaultIncrBy()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            var timeStamp = db.TimeSeriesIncrBy(_key, value);
            Assert.True(timeStamp.UnixMilliseconds > 0);
            Assert.Equal(value, db.TimeSeriesGet(_key).Val);
        }

        [Fact]
        public void TestIncrByTimeStamp()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            TsTimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesIncrBy(_key, timeStamp, value));
            Assert.Equal(new TimeSeriesTuple(timeStamp, value), db.TimeSeriesGet(_key));
        }

        [Fact]
        public void TestDefaultIncrByWithRetentionTime()
        {
            double value = 5.5;
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            var timeStamp = db.TimeSeriesIncrBy(_key, value, retentionTime: retentionTime);
            Assert.True(timeStamp.UnixMilliseconds > 0);
            Assert.Equal(value, db.TimeSeriesGet(_key).Val);
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestDefaultIncrByWithLabels()
        {
            double value = 5.5;
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            IDatabase db = redisFixture.Redis.GetDatabase();
            var labels = new List<TimeSeriesLabel> { label };
            var timeStamp = db.TimeSeriesIncrBy(_key, value, labels: labels);
            Assert.True(timeStamp.UnixMilliseconds > 0);
            Assert.Equal(value, db.TimeSeriesGet(_key).Val);
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestDefaultIncrByWithUncompressed()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            var timeStamp = db.TimeSeriesIncrBy(_key, value, uncompressed: true);
            Assert.True(timeStamp.UnixMilliseconds > 0);
            Assert.Equal(value, db.TimeSeriesGet(_key).Val);
        }
    }
}
