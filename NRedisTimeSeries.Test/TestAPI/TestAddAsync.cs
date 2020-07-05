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
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
        }

        [Fact]
        public async Task TestAddExistingTimeSeries()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(key);
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
        }

        [Fact]
        public async Task TestAddStar()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesAddAsync(key, "*", 1.1);
            var info = await db.TimeSeriesInfoAsync(key);
            Assert.True(info.FirstTimeStamp > 0);
            Assert.Equal(info.FirstTimeStamp, info.LastTimeStamp);
        }

        [Fact]
        public async Task TestAddWithRetentionTime()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            long retentionTime = 5000;
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1, retentionTime: retentionTime));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public async Task TestAddWithLabels()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1, labels: labels));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public async Task TestAddWithUncompressed()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(key);
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1, uncompressed: true));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
        }


        [Fact]
        public async Task TestOldAdd()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var dateTime = DateTime.UtcNow;
            TimeStamp oldTimeStamp = dateTime.AddSeconds(-1);
            TimeStamp newTimeStamp = dateTime;
            await db.TimeSeriesCreateAsync(key);
            await db.TimeSeriesAddAsync(key, newTimeStamp, 1.1);
            // Adding old event
            await db.TimeSeriesAddAsync(key, oldTimeStamp, 1.1);
        }

        [Fact]
        public async Task TestWrongParameters()
        {
            var key = CreateKeyName();
            var value = 1.1;
            var db = redisFixture.Redis.GetDatabase();
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesAddAsync(key, "+", value));
            Assert.Equal("TSDB: invalid timestamp", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesAddAsync(key, "-", value));
            Assert.Equal("TSDB: invalid timestamp", ex.Message);
        }
    }
}
