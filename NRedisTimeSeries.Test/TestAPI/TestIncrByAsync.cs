using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestIncrByAsync : AbstractTimeSeriesTest
    {
        public TestIncrByAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestDefualtIncrBy()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesIncrByAsync(key, value) > 0);
            
            var result = await db.TimeSeriesGetAsync(key);
            Assert.Equal(value, result.Val);
        }

        [Fact]
        public async Task TestStarIncrBy()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesIncrByAsync(key, value, timestamp: "*") > 0);
            
            var result = await db.TimeSeriesGetAsync(key);
            Assert.Equal(value, result.Val);
        }

        [Fact]
        public async Task TestIncrByTimeStamp()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            var timeStamp = new TimeStamp(DateTime.UtcNow);
            Assert.Equal(timeStamp, await db.TimeSeriesIncrByAsync(key, value, timestamp: timeStamp));
            Assert.Equal(new TimeSeriesTuple(timeStamp, value), await db.TimeSeriesGetAsync(key));
        }

        [Fact]
        public async Task TestDefualtIncrByWithRetentionTime()
        {
            var key = CreateKeyName();
            var value = 5.5;
            long retentionTime = 5000;
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesIncrByAsync(key, value, retentionTime: retentionTime) > 0);
            
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
            Assert.True(await db.TimeSeriesIncrByAsync(key, value, labels: labels) > 0);
            
            var result = await db.TimeSeriesGetAsync(key);
            Assert.Equal(value, result.Val);
            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public async Task TestDefualtIncrByWithUncompressed()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesIncrByAsync(key, value, uncompressed:true) > 0);
            
            var result = await db.TimeSeriesGetAsync(key);
            Assert.Equal(value, result.Val);
        }

        [Fact]
        public async Task TestWrongParameters()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesIncrByAsync(key, value, timestamp: "+"));
            Assert.Equal("TSDB: invalid timestamp", ex.Message);
            
            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesIncrByAsync(key, value, timestamp: "-"));
            Assert.Equal("TSDB: invalid timestamp", ex.Message);
        }
    }
}
