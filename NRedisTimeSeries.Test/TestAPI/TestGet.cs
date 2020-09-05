using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestGet : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string _key = "GET_TESTS";

        public TestGet(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(_key);
        }

        [Fact]
        public void TestGetNotExists()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var ex = Assert.Throws<RedisServerException>(()=>db.TimeSeriesGet(_key));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public void TestEmptyGet()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(_key);
            Assert.Null(db.TimeSeriesGet(_key));
        }

        [Fact]
        public void TestAddAndGet()
        {
            DateTime now = DateTime.UtcNow;
            TimeSeriesTuple expected = new TimeSeriesTuple(now, 1.1);
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(_key);
            db.TimeSeriesAdd(_key, now, 1.1);
            TimeSeriesTuple actual = db.TimeSeriesGet(_key);
            Assert.Equal(expected, actual);
        }
    }
}
