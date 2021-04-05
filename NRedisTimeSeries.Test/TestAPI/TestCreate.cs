using System;
using System.Collections.Generic;
using NRedisTimeSeries;
using NRedisTimeSeries.DataTypes;
using NRedisTimeSeries.Commands;
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
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestCreateOK()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
        }

        [Fact]
        public void TestCreateRetentionTime()
        {
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key, retentionTime: retentionTime));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestCreateLabels()
        {
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            IDatabase db = redisFixture.Redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key, labels: labels));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestCreateEmptyLabels()
        {
            var labels = new List<TimeSeriesLabel>();
            IDatabase db = redisFixture.Redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key, labels: labels));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestCreateUncompressed()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key, uncompressed: true));
        }

        [Fact]
        public void TestCreatehDuplicatePolicy()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key, policy: TsDuplicatePolicy.FIRST));
            //Assert.True(db.TimeSeriesCreate(key, policy: TsDuplicatePolicy.LAST));
            //Assert.True(db.TimeSeriesCreate(key, policy: TsDuplicatePolicy.MIN));
            //Assert.True(db.TimeSeriesCreate(key, policy: TsDuplicatePolicy.MAX));
            //Assert.True(db.TimeSeriesCreate(key, policy: TsDuplicatePolicy.SUM));
        }
    }
}
