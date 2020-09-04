using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestIncrByAsync : AbstractTimeSeriesTest
    {
        public TestIncrByAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestDefaultIncrBy()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            var timeStamp = await db.TimeSeriesIncrByAsync(key, value);
            Assert.True(timeStamp.UnixMilliseconds > 0);

            var result = await db.TimeSeriesGetAsync(key);
            Assert.Equal(value, result.Val);
        }

        [Fact]
        public async Task TestIncrByTimeStamp()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            TsTimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await db.TimeSeriesIncrByAsync(key, timeStamp, value));
            Assert.Equal(new TimeSeriesTuple(timeStamp, value), await db.TimeSeriesGetAsync(key));
        }

        [Fact]
        public async Task TestDefaultIncrByWithRetentionTime()
        {
            var key = CreateKeyName();
            var value = 5.5;
            long retentionTime = 5000;
            var db = redisFixture.Redis.GetDatabase();
            var timeStamp = await db.TimeSeriesIncrByAsync(key, value, retentionTime: retentionTime);
            Assert.True(timeStamp.UnixMilliseconds > 0);

            var result = await db.TimeSeriesGetAsync(key);
            Assert.Equal(value, result.Val);

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public async Task TestDefaultIncrByWithLabels()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var label = new TimeSeriesLabel("key", "value");
            var db = redisFixture.Redis.GetDatabase();
            var labels = new List<TimeSeriesLabel> { label };
            var timeStamp = await db.TimeSeriesIncrByAsync(key, value, labels: labels);
            Assert.True(timeStamp.UnixMilliseconds > 0);

            var result = await db.TimeSeriesGetAsync(key);
            Assert.Equal(value, result.Val);

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public async Task TestDefaultIncrByWithUncompressed()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            var timeStamp = await db.TimeSeriesIncrByAsync(key, value, uncompressed: true);
            Assert.True(timeStamp.UnixMilliseconds > 0);

            var result = await db.TimeSeriesGetAsync(key);
            Assert.Equal(value, result.Val);
        }
    }
}
