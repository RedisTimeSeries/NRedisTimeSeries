using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestGetAsync : AbstractTimeSeriesTest
    {
        public TestGetAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestGetNotExists()
        {
            var key = CreateKey();
            var db = redisFixture.Redis.GetDatabase();
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesGetAsync(key));
            Assert.Equal("TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public async Task TestEmptyGet()
        {
            var key = CreateKey();
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(key);
            Assert.Null(await db.TimeSeriesGetAsync(key));
        }

        [Fact]
        public async Task TestAddAndGet()
        {
            var key = CreateKey();
            var now = DateTime.UtcNow;
            var expected = new TimeSeriesTuple(now, 1.1);
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(key);
            await db.TimeSeriesAddAsync(key, now, 1.1);
            var actual = await db.TimeSeriesGetAsync(key);
            Assert.Equal(expected, actual);
        }
    }
}
