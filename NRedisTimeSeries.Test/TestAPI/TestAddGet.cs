using System;
using System.Threading;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestAddGet : IDisposable, IClassFixture<RedisFixture>
    {

        private RedisFixture redisFixture;

        private readonly string keyname = "ADD_GET_ts1";

        public TestAddGet(RedisFixture redisFixture) => this.redisFixture = redisFixture;

        public void Dispose()
        {
            redisFixture.redis.GetDatabase().KeyDelete(keyname);
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
            var ex = Assert.Throws<RedisServerException>(()=>db.TimeSeriesGet(keyname));
            Assert.Equal("TSDB: key does not exist", ex.Message);
        }

        [Fact]
        public void TestEmptyGet()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            db.TimeSeriesCreate(keyname);
            Assert.Null(db.TimeSeriesGet(keyname));
            db.KeyDelete(keyname);
        }

        [Fact]
        public void TestAddAndGet()
        {
            DateTime now = DateTime.Now;
            TimeSeriesTuple expected = new TimeSeriesTuple(now, 1.1);
            IDatabase db = redisFixture.redis.GetDatabase();
            db.TimeSeriesCreate(keyname);
            db.TimeSeriesAdd(keyname, now, 1.1);
            TimeSeriesTuple actual = db.TimeSeriesGet(keyname);
            Assert.Equal(expected, actual);
            db.KeyDelete(keyname);

        }

        [Fact]
        public void TestOldAdd()
        {
            DateTime old_dt = DateTime.Now;
            Thread.Sleep(1000);
            DateTime new_dt = DateTime.Now;
            IDatabase db = redisFixture.redis.GetDatabase();
            db.TimeSeriesCreate(keyname);
            db.TimeSeriesAdd(keyname, new_dt, 1.1);
            var ex = Assert.Throws<RedisServerException>(() => db.TimeSeriesAdd(keyname, old_dt, 1.2));
            Assert.Equal("TSDB: Timestamp cannot be older than the latest timestamp in the time series", ex.Message);
            db.KeyDelete(keyname);
        }
    }
}
