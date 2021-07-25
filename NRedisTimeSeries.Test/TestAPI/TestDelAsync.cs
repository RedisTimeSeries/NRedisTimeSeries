using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestDelAsync : AbstractTimeSeriesTest
    {
        public TestDelAsync(RedisFixture redisFixture) : base(redisFixture) { }

        private async Task<List<TimeSeriesTuple>> CreateData(IDatabase db, string key, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (var i = 0; i < 10; i++)
            {
                var ts = await db.TimeSeriesAddAsync(key, i * timeBucket, i);
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        [Fact]
        public async Task TestDelNotExists()
        {
            var key = CreateKeyName();
            IDatabase db = redisFixture.Redis.GetDatabase();
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesDelAsync(key, "-", "+"));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public async Task TestAddAndDel()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var key = CreateKeyName();
            var tuples = CreateData(db, key, 50).Result;
            TimeStamp from = tuples[0].Time;
            TimeStamp to = tuples[5].Time;
            Assert.True(await db.TimeSeriesDelAsync(key, from, to));
            
            //check that the operation deleted the timestamps
            IReadOnlyList<TimeSeriesTuple> res = await db.TimeSeriesRangeAsync(key, from, to);
            Assert.Equal(0, res.Count);
            Assert.NotNull(await db.TimeSeriesGetAsync(key));
        }
    }
}
