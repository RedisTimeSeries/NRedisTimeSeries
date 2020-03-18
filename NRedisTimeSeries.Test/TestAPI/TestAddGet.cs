using System;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestAddGet : IDisposable, IClassFixture<RedisFixture>
    {

        private RedisFixture redisFixture;

        private readonly string keyname = "ts1";

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
            Assert.Equal(now, (DateTime)db.TimeSeriesAdd("ts2", now, 1.1));
            db.KeyDelete("ts2");
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
        }

        [Fact]
        public void TestAddAndGet()
        {
            DateTime now = DateTime.Now;
            Value expected = new Value(now, 1.1);
            IDatabase db = redisFixture.redis.GetDatabase();
            db.TimeSeriesCreate(keyname);
            db.TimeSeriesAdd(keyname, now, 1.1);
            Value actual = db.TimeSeriesGet(keyname);
            Assert.Equal(expected, actual);
        }
    }
}
