using System;
using System.Collections.Generic;
using System.Threading;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestAdd : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string _key = "ADD_TESTS";

        public TestAdd(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(_key);
        }

        [Fact]
        public void TestAddNotExistingTimeSeries()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TsTimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TimeSeriesAdd(_key, now, 1.1));
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
        }

        [Fact]
        public void TestAddExistingTimeSeries()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(_key);
            TsTimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TimeSeriesAdd(_key, now, 1.1));
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
        }

        [Fact]
        public void TestAddStar()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesAdd(_key, 1.1);
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.True(info.FirstTimeStamp.UnixMilliseconds > 0);
            Assert.Equal(info.FirstTimeStamp, info.LastTimeStamp);
        }

        [Fact]
        public void TestAddWithRetentionTime()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TsTimeStamp now = DateTime.UtcNow;
            long retentionTime = 5000;
            Assert.Equal(now, db.TimeSeriesAdd(_key, now, 1.1, retentionTime: retentionTime));
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestAddWithLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TsTimeStamp now = DateTime.UtcNow;
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            Assert.Equal(now, db.TimeSeriesAdd(_key, now, 1.1, labels: labels));
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestAddWithUncompressed()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(_key);
            TsTimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TimeSeriesAdd(_key, now, 1.1, uncompressed: true));
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
        }

        [Fact]
        public void TestAddWithChunkSize()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TsTimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TimeSeriesAdd(_key, now, 1.1, chunkSizeBytes: 128));
            TimeSeriesInformation info = db.TimeSeriesInfo(_key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
            Assert.Equal(128, info.ChunkSize);
        }

        [Fact]
        public void TestOldAdd()
        {
            TsTimeStamp old_dt = DateTime.UtcNow;
            Thread.Sleep(1000);
            TsTimeStamp new_dt = DateTime.UtcNow;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(_key);
            db.TimeSeriesAdd(_key, new_dt, 1.1);
            // Adding old event
            Assert.Equal(old_dt, db.TimeSeriesAdd(_key, old_dt, 1.1));
        }
    }
}
