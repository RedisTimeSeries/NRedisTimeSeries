using System;
using System.Collections.Generic;
using System.Threading;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestRevRange : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "REVRANGE_TESTS";

        public TestRevRange(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        private List<TimeSeriesTuple> CreateData(IDatabase db, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (int i = 0; i < 10; i++)
            {
                TimeStamp ts = db.TimeSeriesAdd(key, i*timeBucket, i);
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        private List<TimeSeriesTuple> ReverseData(List<TimeSeriesTuple> data)
        {
            var tuples = new List<TimeSeriesTuple>();

            for (int i = data.Count - 1; i >= 0; i--)
            {
                tuples.Add(data[i]);
            }
            return tuples;
        }

        [Fact]
        public void TestSimpleRevRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            Assert.Equal(ReverseData(tuples), db.TimeSeriesRevRange(key, "-", "+"));
        }

        [Fact]
        public void TestRevRangeCount()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            Assert.Equal(ReverseData(tuples).GetRange(0, 5), db.TimeSeriesRevRange(key, "-", "+", count: 5));
        }

        [Fact]
        public void TestRevRangeAggregation()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            Assert.Equal(ReverseData(tuples), db.TimeSeriesRevRange(key, "-", "+", aggregation: Aggregation.MIN, timeBucket: 50));
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesRevRange(key, "-", "+", aggregation: Aggregation.AVG));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);

        }
    }
}
