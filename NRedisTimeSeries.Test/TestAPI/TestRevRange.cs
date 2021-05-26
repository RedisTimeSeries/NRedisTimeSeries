using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestRevRange : AbstractTimeSeriesTest
    {
        public TestRevRange(RedisFixture redisFixture) : base(redisFixture) { }

        private List<TimeSeriesTuple> CreateData(IDatabase db, string key, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (var i = 0; i < 10; i++)
            {
                var ts = db.TimeSeriesAdd(key, i * timeBucket, i);
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        [Fact]
        public void TestSimpleRevRange()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, key, 50);
            Assert.Equal(ReverseData(tuples), db.TimeSeriesRevRange(key, "-", "+"));
        }

        [Fact]
        public void TestRevRangeCount()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, key, 50);
            Assert.Equal(ReverseData(tuples).GetRange(0, 5), db.TimeSeriesRevRange(key, "-", "+", count: 5));
        }

        [Fact]
        public void TestRevRangeAggregation()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, key, 50);
            Assert.Equal(ReverseData(tuples), db.TimeSeriesRevRange(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, key, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesRevRange(key, "-", "+", aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);

        }

        [Fact]
        public void TestFilterBy()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, key, 50);

            var res = db.TimeSeriesRevRange(key, "-", "+", filterByValue: (0,2)); 
            Assert.Equal(res.Count, 3);
            Assert.Equal(res, ReverseData(tuples.GetRange(0,3)));

            var filterTs = new List<TimeStamp> {0, 50, 100}; 
            res = db.TimeSeriesRevRange(key, "-", "+", filterByTs: filterTs); 
            Assert.Equal(res, ReverseData(tuples.GetRange(0,3)));

            res = db.TimeSeriesRevRange(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5));
            Assert.Equal(res, tuples.GetRange(2,1));
        }
    }
}
