using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestRevRangeAsync : AbstractTimeSeriesTest
    {
        public TestRevRangeAsync(RedisFixture redisFixture) : base(redisFixture) { }

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
        public async Task TestSimpleRevRange()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = await CreateData(db, key, 50);
            Assert.Equal(ReverseData(tuples), await db.TimeSeriesRevRangeAsync(key, "-", "+"));
        }

        [Fact]
        public async Task TestRevRangeCount()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = await CreateData(db, key, 50);
            Assert.Equal(ReverseData(tuples).GetRange(0, 5), await db.TimeSeriesRevRangeAsync(key, "-", "+", count: 5));
        }

        [Fact]
        public async Task TestRevRangeAggregation()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = await CreateData(db, key, 50);
            Assert.Equal(ReverseData(tuples), await db.TimeSeriesRevRangeAsync(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
        }

        [Fact]
        public async Task TestMissingTimeBucket()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = await CreateData(db, key, 50);
            var ex = await Assert.ThrowsAsync<ArgumentException>(async () => await db.TimeSeriesRevRangeAsync(key, "-", "+", aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public async Task TestFilterBy()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = await CreateData(db, key, 50);

            var res = await db.TimeSeriesRevRangeAsync(key, "-", "+", filterByValue: (0,2)); 
            Assert.Equal(3, res.Count);
            Assert.Equal(ReverseData(tuples.GetRange(0,3)), res);

            var filterTs = new List<TimeStamp> {0, 50, 100}; 
            res = await db.TimeSeriesRevRangeAsync(key, "-", "+", filterByTs: filterTs); 
            Assert.Equal(ReverseData(tuples.GetRange(0,3)), res);

            res = await db.TimeSeriesRevRangeAsync(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5));
            Assert.Equal(tuples.GetRange(2,1), res);
        }        
    }
}
