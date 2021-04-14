using NRedisTimeSeries.DataTypes;
using NRedisTimeSeries.Enums;
using NRedisTimeSeries.Commands;
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
        public async Task TestAddWithChunkSize()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1, chunkSizeBytes: 128));
            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
            Assert.Equal(128, info.ChunkSize);
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
        public async Task TestAddWithDuplicatePolicyBlock() 
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1));

            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesAddAsync(key, timeStamp, 1.2));
            Assert.Equal("ERR TSDB: Error at upsert, update is not supported in BLOCK mode", ex.Message);
        }

        [Fact]
        public async Task TestAddWithDuplicatePolicyMin()
        { 
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1));

            // Insert a bigger number and check that it did not change the value.
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.2, policy: TsDuplicatePolicy.MIN));
            IReadOnlyList<TimeSeriesTuple> results = await db.TimeSeriesRangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.1, results[0].Val);

            // Insert a smaller number and check that it changed.
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.0, policy: TsDuplicatePolicy.MIN));
            results = await db.TimeSeriesRangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.0, results[0].Val);
        }

        [Fact]
        public async Task TestAddWithDuplicatePolicyMax()
        { 
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1));

            // Insert a smaller number and check that it did not change the value.
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.0, policy: TsDuplicatePolicy.MAX));
            IReadOnlyList<TimeSeriesTuple> results = await db.TimeSeriesRangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.1, results[0].Val);
            // Insert a bigger number and check that it changed.
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.2, policy: TsDuplicatePolicy.MAX));
            results = await db.TimeSeriesRangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.2, results[0].Val);
        }

        [Fact]
        public async Task TestAddWithDuplicatePolicySum()
        { 
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1));
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.0, policy: TsDuplicatePolicy.SUM));
            IReadOnlyList<TimeSeriesTuple> results = await db.TimeSeriesRangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(2.1, results[0].Val);
        }

        [Fact]
        public async Task TestAddWithDuplicatePolicyFirst()
        { 
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1));
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.0, policy: TsDuplicatePolicy.FIRST));
            IReadOnlyList<TimeSeriesTuple> results = await db.TimeSeriesRangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.1, results[0].Val);
        }

        [Fact]
        public async Task TestAddWithDuplicatePolicyLast()
        { 
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.1));
            Assert.Equal(timeStamp, await db.TimeSeriesAddAsync(key, timeStamp, 1.0, policy: TsDuplicatePolicy.LAST));
            IReadOnlyList<TimeSeriesTuple> results = await db.TimeSeriesRangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.0, results[0].Val);
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
            Assert.Equal( oldTimeStamp, await db.TimeSeriesAddAsync(key, oldTimeStamp, 1.1));
        }

        [Fact]
        public async Task TestWrongParameters()
        {
            var key = CreateKeyName();
            var value = 1.1;
            var db = redisFixture.Redis.GetDatabase();
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesAddAsync(key, "+", value));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesAddAsync(key, "-", value));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
        }
    }
}
