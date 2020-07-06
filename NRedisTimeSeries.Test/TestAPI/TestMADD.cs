using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestMADD : AbstractTimeSeriesTest, IDisposable
    {

        private readonly string[] keys = { "MADD_TESTS_1", "MADD_TESTS_2" };

        public TestMADD(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            foreach(string key in keys)
            {
                redisFixture.Redis.GetDatabase().KeyDelete(key);
            }
        }

        [Fact]
        public void TestSuccessfulMADD()
        {

            IDatabase db = redisFixture.Redis.GetDatabase();

            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key);
            }

            List<(string, TimeStamp, double)> sequence = new List<(string, TimeStamp, double)>(keys.Length);
            List<DateTime> timestamps = new List<DateTime>(keys.Length);
            foreach (var keyname in keys)
            {
                DateTime now = DateTime.UtcNow;
                timestamps.Add(now);
                sequence.Add((keyname, now, 1.1));
            }
            var response = db.TimeSeriesMAdd(sequence);

            Assert.Equal(timestamps.Count, response.Count);
            for(int i = 0; i < response.Count; i++)
            {
                Assert.Equal<DateTime>(timestamps[i], response[i]);
            }
        }

        [Fact]
        public void TestOverrideMADD()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();

            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key);
            }

            List<DateTime> oldTimeStamps = new List<DateTime>();
            foreach (var keyname in keys)
            {
                oldTimeStamps.Add(DateTime.UtcNow);
            }

            List<(string, TimeStamp, double)> sequence = new List<(string, TimeStamp, double)>(keys.Length);
            foreach (var keyname in keys)
            {
                sequence.Add((keyname, DateTime.UtcNow, 1.1));
            }
            db.TimeSeriesMAdd(sequence);

            sequence.Clear();

            // Override the same events should not throw an error
            for (int i =0; i < keys.Length; i++)
            {
                sequence.Add((keys[i], oldTimeStamps[i], 1.1));
            }
            var response = db.TimeSeriesMAdd(sequence);

            Assert.Equal(oldTimeStamps.Count, response.Count);
            for(int i = 0; i < response.Count; i++)
            {
                Assert.Equal<DateTime>(oldTimeStamps[i], response[i]);
            }
        }
    }
}
