﻿using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands.Enums;
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

        [Fact]
        public void TestSimpleRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            Assert.Equal(tuples, db.TimeSeriesRange(key, "-", "+"));
        }

        [Fact]
        public void TestRangeCount()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            Assert.Equal(tuples.GetRange(0, 5), db.TimeSeriesRange(key, "-", "+", count: 5));
        }

        [Fact]
        public void TestRangeAggregation()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            Assert.Equal(tuples, db.TimeSeriesRange(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
        }

        [Fact]
        public void TestRangeAlign()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(1, 10),
                new TimeSeriesTuple(3, 5),
                new TimeSeriesTuple(11, 10),
                new TimeSeriesTuple(21, 11)
            };

            foreach (var tuple in tuples)
            {
                db.TimeSeriesAdd(key, tuple.Time, tuple.Val);
            }

            // Aligh start
            var resStart = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(1, 2),
                new TimeSeriesTuple(11, 1),
                new TimeSeriesTuple(21, 1)
            };
            Assert.Equal(resStart, db.TimeSeriesRange(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));
                        
            // Aligh end
            var resEnd = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(0, 2),
                new TimeSeriesTuple(10, 1),
                new TimeSeriesTuple(20, 1)
            };
            Assert.Equal(resEnd, db.TimeSeriesRange(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

            // Align 1
            Assert.Equal(resStart, db.TimeSeriesRange(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesRange(key, "-", "+", aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public void TestFilterBy()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);

            var res = db.TimeSeriesRange(key, "-", "+", filterByValue: (0,2)); // The first 3 tuples
            Assert.Equal(3, res.Count);
            Assert.Equal(tuples.GetRange(0,3), res);

            var filterTs = new List<TimeStamp> {0, 50, 100}; // Also the first 3 tuples
            res = db.TimeSeriesRange(key, "-", "+", filterByTs: filterTs); 
            Assert.Equal(tuples.GetRange(0,3), res);

            res = db.TimeSeriesRange(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5)); // The third tuple
            Assert.Equal(tuples.GetRange(2,1), res);
        }
    }
}
