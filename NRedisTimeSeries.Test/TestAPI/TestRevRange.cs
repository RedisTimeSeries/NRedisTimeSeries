using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands;
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
            Assert.Equal(ReverseData(tuples), db.TimeSeriesRevRange(key, "-", "+", aggregation: Aggregation.MIN, timeBucket: 50));
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, key, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesRevRange(key, "-", "+", aggregation: Aggregation.AVG));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);

        }
    }
}
