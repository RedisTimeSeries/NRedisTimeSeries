using System;
using System.Threading;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestAddGet : IDisposable, IClassFixture<RedisFixture>
    {

        private RedisFixture redisFixture;

        private readonly string key = "ADD_GET_ts1";

        public TestAddGet(RedisFixture redisFixture) => this.redisFixture = redisFixture;

        public void Dispose()
        {
            redisFixture.redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestAddNotExists()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            DateTime now = DateTime.Now;
            Assert.Equal(now, (DateTime)db.TimeSeriesAdd("ADD_GET_ts2", now, 1.1));
            db.KeyDelete("ADD_GET_ts2");
        }

        [Fact]
        public void TestGetNotExists()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            var ex = Assert.Throws<RedisServerException>(()=>db.TimeSeriesGet(key));
            Assert.Equal("TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public void TestEmptyGet()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            db.TimeSeriesCreate(key);
            Assert.Null(db.TimeSeriesGet(key));
        }

        [Fact]
        public void TestAddAndGet()
        {
            DateTime now = DateTime.Now;
            TimeSeriesTuple expected = new TimeSeriesTuple(now, 1.1);
            IDatabase db = redisFixture.redis.GetDatabase();
            db.TimeSeriesCreate(key);
            db.TimeSeriesAdd(key, now, 1.1);
            TimeSeriesTuple actual = db.TimeSeriesGet(key);
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void TestOldAdd()
        {
            DateTime old_dt = DateTime.Now;
            Thread.Sleep(1000);
            DateTime new_dt = DateTime.Now;
            IDatabase db = redisFixture.redis.GetDatabase();
            db.TimeSeriesCreate(key);
            db.TimeSeriesAdd(key, new_dt, 1.1);
            var ex = Assert.Throws<RedisServerException>(() => db.TimeSeriesAdd(key, old_dt, 1.2));
            Assert.Equal("TSDB: Timestamp cannot be older than the latest timestamp in the time series", ex.Message);
        }
    }
}
