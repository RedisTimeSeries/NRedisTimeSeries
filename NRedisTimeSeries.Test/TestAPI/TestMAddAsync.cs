using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestMAddAsync : AbstractTimeSeriesTest
    {
        public TestMAddAsync(RedisFixture redisFixture) : base(redisFixture) { }


        [Fact]
        public async Task TestStarMADD()
        {
            var keys = CreateKeyNames(2);

            IDatabase db = redisFixture.Redis.GetDatabase();

            foreach (string key in keys)
            {
                await db.TimeSeriesCreateAsync(key);
            }

            List<(string, TimeStamp, double)> sequence = new List<(string, TimeStamp, double)>(keys.Length);
            foreach (var keyname in keys)
            {
                sequence.Add((keyname, "*", 1.1));
            }
            var response = await db.TimeSeriesMAddAsync(sequence);

            Assert.Equal(keys.Length, response.Count);

            foreach (var key in keys)
            {
                TimeSeriesInformation info = await db.TimeSeriesInfoAsync(key);
                Assert.True(info.FirstTimeStamp > 0);
                Assert.Equal(info.FirstTimeStamp, info.LastTimeStamp);
            }
        }


        [Fact]
        public async Task TestSuccessfulMAdd()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();

            foreach (var key in keys)
            {
                await db.TimeSeriesCreateAsync(key);
            }

            var sequence = new List<(string, TimeStamp, double)>(keys.Length);
            var timestamps = new List<DateTime>(keys.Length);
            foreach (var keyname in keys)
            {
                var now = DateTime.UtcNow;
                timestamps.Add(now);
                sequence.Add((keyname, now, 1.1));
            }

            var response = await db.TimeSeriesMAddAsync(sequence);
            Assert.Equal(timestamps.Count, response.Count);
            for (var i = 0; i < response.Count; i++)
            {
                Assert.Equal<DateTime>(timestamps[i], response[i]);
            }
        }

        [Fact]
        public async Task TestFailedMAdd()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();

            foreach (var key in keys)
            {
                await db.TimeSeriesCreateAsync(key);
            }

            var oldTimeStamps = new List<DateTime>();
            foreach (var keyname in keys)
            {
                oldTimeStamps.Add(DateTime.UtcNow);
            }

            var sequence = new List<(string, TimeStamp, double)>(keys.Length);
            foreach (var keyname in keys)
            {
                sequence.Add((keyname, DateTime.UtcNow, 1.1));
            }

            await db.TimeSeriesMAddAsync(sequence);
            sequence.Clear();

            for (var i = 0; i < keys.Length; i++)
            {
                sequence.Add((keys[i], oldTimeStamps[i], 1.1));
            }

            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesMAddAsync(sequence));
            Assert.Equal("TSDB: Timestamp cannot be older than the latest timestamp in the time series", ex.Message);
        }
    }
}
