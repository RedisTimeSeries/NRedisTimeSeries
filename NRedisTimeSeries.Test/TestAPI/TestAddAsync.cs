using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestAddAsync : AbstractTimeSeriesTest
    {
        public TestAddAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestAddNotExistingTimeSeries()
        {
            var key = CreateKey();
            var db = redisFixture.Redis.GetDatabase();
            var now = new TimeStamp(DateTime.UtcNow);
            Assert.Equal(now, await db.TimeSeriesAddAsync(key, now, 1.1));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStap);
        }

        [Fact]
        public async Task TestAddExistingTimeSeries()
        {
            var key = CreateKey();
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(key);
            var now = new TimeStamp(DateTime.UtcNow);
            Assert.Equal(now, await db.TimeSeriesAddAsync(key, now, 1.1));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStap);
        }

        [Fact]
        public async Task TestAddStar()
        {
            var key = CreateKey();
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesAddAsync(key, "*", 1.1);
            var info = await db.TimeSeriesInfoAsync(key);
            Assert.True(info.FirstTimeStamp > 0);
            Assert.Equal(info.FirstTimeStamp, info.LastTimeStap);
        }

        [Fact]
        public async Task TestAddWithRetentionTime()
        {
            var key = CreateKey();
            var db = redisFixture.Redis.GetDatabase();
            var now = new TimeStamp(DateTime.UtcNow);
            long retentionTime = 5000;
            Assert.Equal(now, await db.TimeSeriesAddAsync(key, now, 1.1, retentionTime: retentionTime));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStap);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public async Task TestAddWithLabels()
        {
            var key = CreateKey();
            var db = redisFixture.Redis.GetDatabase();
            var now = new TimeStamp(DateTime.UtcNow);
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            Assert.Equal(now, await db.TimeSeriesAddAsync(key, now, 1.1, labels: labels));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStap);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public async Task TestAddWithUncompressed()
        {
            var key = CreateKey();
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(key);
            var now = new TimeStamp(DateTime.UtcNow);
            Assert.Equal(now, await db.TimeSeriesAddAsync(key, now, 1.1, uncompressed: true));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStap);
        }


        [Fact]
        public async Task TestOldAdd()
        {
            var key = CreateKey();
            var db = redisFixture.Redis.GetDatabase();
            var dateTime = DateTime.UtcNow;
            var old_dt = new TimeStamp(dateTime.AddSeconds(-1));
            var new_dt = new TimeStamp(dateTime);
            await db.TimeSeriesCreateAsync(key);
            await db.TimeSeriesAddAsync(key, new_dt, 1.1);
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesAddAsync(key, old_dt, 1.1));
            Assert.Equal("TSDB: Timestamp cannot be older than the latest timestamp in the time series", ex.Message);
        }

        [Fact]
        public async Task TestWrongParameters()
        {
            var key = CreateKey();
            var value = 1.1;
            var db = redisFixture.Redis.GetDatabase();
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesAddAsync(key, "+", value));
            Assert.Equal("TSDB: invalid timestamp", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesAddAsync(key, "-", value));
            Assert.Equal("TSDB: invalid timestamp", ex.Message);
        }
    }
}
