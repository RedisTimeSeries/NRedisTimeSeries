using System;
using System.Threading;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestGet : AbstractTimeSeriesTest, IDisposable
    {

        private readonly string key = "GET_TESTS";

        public TestGet(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestGetNotExists()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var ex = Assert.Throws<RedisServerException>(()=>db.TimeSeriesGet(key));
            Assert.Equal("TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public void TestEmptyGet()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(key);
            Assert.Null(db.TimeSeriesGet(key));
        }

        [Fact]
        public void TestAddAndGet()
        {
            DateTime now = DateTime.Now;
            TimeSeriesTuple expected = new TimeSeriesTuple(now, 1.1);
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(key);
            db.TimeSeriesAdd(key, now, 1.1);
            TimeSeriesTuple actual = db.TimeSeriesGet(key);
            Assert.Equal(expected, actual);
        }
    }
}
