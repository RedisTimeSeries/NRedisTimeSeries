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

            List<(string, double)> sequence = new List<(string, double)>(keys.Length);
            foreach (var keyname in keys)
            {
                sequence.Add((keyname, 1.1));
            }
            var response = await db.TimeSeriesMAddAsync(sequence);

            Assert.Equal(keys.Length, response.Count);

            foreach (var key in keys)
            {
                TimeSeriesInformation info = await db.TimeSeriesInfoAsync(key);
                Assert.True(info.FirstTimeStamp.UnixMilliseconds > 0);
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

            var sequence = new List<(string, TsTimeStamp, double)>(keys.Length);
            var timestamps = new List<TsTimeStamp>(keys.Length);
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
                Assert.Equal(timestamps[i], response[i]);
            }
        }

        [Fact]
        public async Task TestOverrideMAdd()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();

            foreach (var key in keys)
            {
                await db.TimeSeriesCreateAsync(key);
            }

            var oldTimeStamps = new List<TsTimeStamp>();
            foreach (var keyname in keys)
            {
                oldTimeStamps.Add(DateTime.UtcNow);
            }

            var sequence = new List<(string, TsTimeStamp, double)>(keys.Length);
            foreach (var keyname in keys)
            {
                sequence.Add((keyname, DateTime.UtcNow, 1.1));
            }

            await db.TimeSeriesMAddAsync(sequence);
            sequence.Clear();

            // Override the same events should not throw an error
            for (var i = 0; i < keys.Length; i++)
            {
                sequence.Add((keys[i], oldTimeStamps[i], 1.1));
            }

            var response = await db.TimeSeriesMAddAsync(sequence);

            Assert.Equal(oldTimeStamps.Count, response.Count);
            for (int i = 0; i < response.Count; i++)
            {
                Assert.Equal(oldTimeStamps[i], response[i]);
            }
        }
    }
}
