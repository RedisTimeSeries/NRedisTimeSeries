using System;
using System.Collections.Generic;
using NRedisTimeSeries;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestCreate : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "CREATE_TESTS";

        public TestCreate(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestCreateOK()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
        }

        [Fact]
        public void TestCreateRetentionTime()
        {
            long retentionTime = 5000;
            IDatabase db = redisFixture.redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key, retentionTime: retentionTime));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestCreateLabels()
        {
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            IDatabase db = redisFixture.redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key, labels: labels));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestCreateEmptyLabels()
        {
            var labels = new List<TimeSeriesLabel>();
            IDatabase db = redisFixture.redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key, labels: labels));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestCreateUncompressed()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key, uncompressed: true));
        }
    }
}
