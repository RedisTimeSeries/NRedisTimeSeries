using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestDecrBy : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "DECRBY_TESTS";

        public TestDecrBy(RedisFixture redisFixture) : base(redisFixture) {}

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestDefaultDecrBy()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            Assert.True(db.TimeSeriesDecrBy(key, -value) > 0);
            Assert.Equal(value, db.TimeSeriesGet(key).Val);
        }

        [Fact]
        public void TestStarDecrBy()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            Assert.True(db.TimeSeriesDecrBy(key, -value, timestamp: "*") > 0);
            Assert.Equal(value, db.TimeSeriesGet(key).Val);
        }

        [Fact]
        public void TestDecrByTimeStamp()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesDecrBy(key, -value, timestamp: timeStamp));
            Assert.Equal(new TimeSeriesTuple(timeStamp, value), db.TimeSeriesGet(key));
        }

        [Fact]
        public void TestDefaultDecrByWithRetentionTime()
        {
            double value = 5.5;
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            Assert.True(db.TimeSeriesDecrBy(key, -value, retentionTime: retentionTime) > 0);
            Assert.Equal(value, db.TimeSeriesGet(key).Val);
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestDefaultDecrByWithLabels()
        {
            double value = 5.5;
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            IDatabase db = redisFixture.Redis.GetDatabase();
            var labels = new List<TimeSeriesLabel> { label };
            Assert.True(db.TimeSeriesDecrBy(key, -value, labels: labels) > 0);
            Assert.Equal(value, db.TimeSeriesGet(key).Val);
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestDefaultDecrByWithUncompressed()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            Assert.True(db.TimeSeriesDecrBy(key, -value, uncompressed: true) > 0);
            Assert.Equal(value, db.TimeSeriesGet(key).Val);
        }

        [Fact]
        public void TestWrongParameters()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            var ex = Assert.Throws<RedisServerException>(() => db.TimeSeriesDecrBy(key, value, timestamp: "+"));
            Assert.Equal("TSDB: invalid timestamp", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.TimeSeriesDecrBy(key, value, timestamp: "-"));
            Assert.Equal("TSDB: invalid timestamp", ex.Message);
        }
    }
}
