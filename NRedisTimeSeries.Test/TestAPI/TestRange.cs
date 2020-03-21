using System;
using System.Collections.Generic;
using System.Threading;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestRange : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "RANGE_TESTS";

        public TestRange(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestSimpleRange()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            var tuples = new List<TimeSeriesTuple>();
            for (int i = 0; i < 10; i++)
            {
                TimeStamp ts = DateTime.Now;
                db.TimeSeriesAdd(key, ts, i);
                tuples.Add(new TimeSeriesTuple(ts, i));
                Thread.Sleep(50);
            }
            Assert.Equal(tuples, db.TimeSeriesRange(key, "-", "+"));
        }

        [Fact]
        public void TestRangeCount()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            var tuples = new List<TimeSeriesTuple>();
            for (int i = 0; i < 10; i++)
            {
                TimeStamp ts = DateTime.Now;
                db.TimeSeriesAdd(key, ts, i);
                tuples.Add(new TimeSeriesTuple(ts, i));
                Thread.Sleep(50);
            }
            Assert.Equal(tuples.GetRange(0, 5), db.TimeSeriesRange(key, "-", "+", count: 5));
        }

        [Fact]
        public void TestRangeAggregation()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            var tuples = new List<TimeSeriesTuple>();
            for (int i = 0; i < 10; i++)
            {
                TimeStamp ts = DateTime.Now;
                db.TimeSeriesAdd(key, ts, i);
                tuples.Add(new TimeSeriesTuple(ts, i));
                Thread.Sleep(50);
            }

            Aggregation[] aggregarions = { Aggregation.AVG, Aggregation.COUNT, Aggregation.FIRST, Aggregation.LAST, Aggregation.MAX, Aggregation.MIN, Aggregation.RANGE, Aggregation.STDP, Aggregation.STDS, Aggregation.SUM, Aggregation.VARP, Aggregation.VARS };
            Array.ForEach(aggregarions, aggregation => Assert.Equal(tuples, db.TimeSeriesRange(key, "-", "+", aggregation: aggregation, timeBucket: 50)));
        }
    }
}
