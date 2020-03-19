using System;
using System.Collections.Generic;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestMADD : IDisposable, IClassFixture<RedisFixture>
    {
        private RedisFixture redisFixture;

        private readonly string[] keynames = { "MADD_ts1", "MADD_ts2" };

        public TestMADD(RedisFixture redisFixture) => this.redisFixture = redisFixture;

        public void Dispose()
        {
            foreach(string keyname in keynames)
            {
                redisFixture.redis.GetDatabase().KeyDelete(keyname);
            }
        }

        [Fact]
        public void TestSuccessfulMADD()
        {

            IDatabase db = redisFixture.redis.GetDatabase();

            foreach (string keyname in keynames)
            {
                db.TimeSeriesCreate(keyname);
            }

            List<(string, TimeStamp, double)> sequence = new List<(string, TimeStamp, double)>(keynames.Length);
            List<DateTime> timestamps = new List<DateTime>(keynames.Length);
            foreach (var keyname in keynames)
            {
                DateTime now = DateTime.Now;
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
        public void TestFailedMADD()
        {
            IDatabase db = redisFixture.redis.GetDatabase();

            foreach (string keyname in keynames)
            {
                db.TimeSeriesCreate(keyname);
            }

            List<DateTime> oldTimeStamps = new List<DateTime>();
            foreach (var keyname in keynames)
            {
                oldTimeStamps.Add(DateTime.Now);
            }

            List<(string, TimeStamp, double)> sequence = new List<(string, TimeStamp, double)>(keynames.Length);
            foreach (var keyname in keynames)
            {
                sequence.Add((keyname, DateTime.Now, 1.1));
            }
            db.TimeSeriesMAdd(sequence);

            sequence.Clear();

            for (int i =0; i < keynames.Length; i++)
            {
                sequence.Add((keynames[i], oldTimeStamps[i], 1.1));
            }

            var ex = Assert.Throws<RedisServerException>(()=>db.TimeSeriesMAdd(sequence));
            Assert.Equal("TSDB: Timestamp cannot be older than the latest timestamp in the time series", ex.Message);
        }
    }
}
