using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestRange : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string _key = "RANGE_TESTS";

        public TestRange(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(_key);
        }

        private List<TimeSeriesTuple> CreateData(IDatabase db, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (int i = 0; i < 10; i++)
            {
                TsTimeStamp ts = db.TimeSeriesAdd(_key, new TsTimeStamp(i * timeBucket), i);
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        [Fact]
        public void TestSimpleRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            Assert.Equal(tuples, db.TimeSeriesRange(_key, TsTimeStamp.MinValue, TsTimeStamp.MaxValue));
        }

        [Fact]
        public void TestRangeCount()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            Assert.Equal(tuples.GetRange(0, 5), db.TimeSeriesRange(_key, TsTimeStamp.MinValue, TsTimeStamp.MaxValue, count: 5));
        }

        [Fact]
        public void TestRangeAggregation()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            Assert.Equal(tuples, db.TimeSeriesRange(_key, TsTimeStamp.MinValue, TsTimeStamp.MaxValue, aggregation: Aggregation.MIN, timeBucket: new TsTimeBucket(50)));
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesRange(_key, TsTimeStamp.MinValue, TsTimeStamp.MaxValue, aggregation: Aggregation.AVG));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);

        }
    }
}
