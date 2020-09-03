using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestMADD : AbstractTimeSeriesTest, IDisposable
    {

        private readonly string[] _keys = { "MADD_TESTS_1", "MADD_TESTS_2" };

        public TestMADD(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            foreach (string key in _keys)
            {
                redisFixture.Redis.GetDatabase().KeyDelete(key);
            }
        }

        [Fact]
        public void TestStarMADD()
        {

            IDatabase db = redisFixture.Redis.GetDatabase();

            foreach (string key in _keys)
            {
                db.TimeSeriesCreate(key);
            }
            List<(string, double)> sequence = new List<(string, double)>(_keys.Length);
            foreach (var keyname in _keys)
            {
                sequence.Add((keyname, 1.1));
            }
            var response = db.TimeSeriesMAdd(sequence);

            Assert.Equal(_keys.Length, response.Count);

            foreach (var key in _keys)
            {
                TimeSeriesInformation info = db.TimeSeriesInfo(key);
                Assert.True(info.FirstTimeStamp.UnixMilliseconds > 0);
                Assert.Equal(info.FirstTimeStamp, info.LastTimeStamp);
            }
        }

        [Fact]
        public void TestSuccessfulMADD()
        {

            IDatabase db = redisFixture.Redis.GetDatabase();

            foreach (string key in _keys)
            {
                db.TimeSeriesCreate(key);
            }

            List<(string, TsTimeStamp, double)> sequence = new List<(string, TsTimeStamp, double)>(_keys.Length);
            List<TsTimeStamp> timestamps = new List<TsTimeStamp>(_keys.Length);
            foreach (var keyname in _keys)
            {
                TsTimeStamp now = DateTime.UtcNow;
                timestamps.Add(now);
                sequence.Add((keyname, now, 1.1));
            }
            var response = db.TimeSeriesMAdd(sequence);

            Assert.Equal(timestamps.Count, response.Count);
            for (int i = 0; i < response.Count; i++)
            {
                Assert.Equal(timestamps[i], response[i]);
            }
        }

        [Fact]
        public void TestOverrideMADD()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();

            foreach (string key in _keys)
            {
                db.TimeSeriesCreate(key);
            }

            List<TsTimeStamp> oldTimeStamps = new List<TsTimeStamp>();
            foreach (var keyname in _keys)
            {
                oldTimeStamps.Add(DateTime.UtcNow);
            }

            List<(string, TsTimeStamp, double)> sequence = new List<(string, TsTimeStamp, double)>(_keys.Length);
            foreach (var keyname in _keys)
            {
                sequence.Add((keyname, DateTime.UtcNow, 1.1));
            }
            db.TimeSeriesMAdd(sequence);

            sequence.Clear();

            // Override the same events should not throw an error
            for (int i = 0; i < _keys.Length; i++)
            {
                sequence.Add((_keys[i], oldTimeStamps[i], 1.1));
            }
            var response = db.TimeSeriesMAdd(sequence);

            Assert.Equal(oldTimeStamps.Count, response.Count);
            for (int i = 0; i < response.Count; i++)
            {
                Assert.Equal(oldTimeStamps[i], response[i]);
            }
        }
    }
}
