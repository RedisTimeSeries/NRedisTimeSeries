﻿using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestRangeAsync : AbstractTimeSeriesTest
    {
        public TestRangeAsync(RedisFixture redisFixture) : base(redisFixture) { }

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
        public async Task TestSimpleRange()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = await CreateData(db, key, 50);
            Assert.Equal(tuples, await db.TimeSeriesRangeAsync(key, "-", "+"));
        }

        [Fact]
        public async Task TestRangeCount()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = await CreateData(db, key, 50);
            Assert.Equal(tuples.GetRange(0, 5), await db.TimeSeriesRangeAsync(key, "-", "+", count: 5));
        }

        [Fact]
        public async Task TestRangeAggregation()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = await CreateData(db, key, 50);
            Assert.Equal(tuples, await db.TimeSeriesRangeAsync(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
        }
        
        [Fact]
        public async Task TestRangeAlign()
        {
            var key = CreateKeyName();
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
                await db.TimeSeriesAddAsync(key, tuple.Time, tuple.Val);
            }

            // Aligh start
            var resStart = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(1, 2),
                new TimeSeriesTuple(11, 1),
                new TimeSeriesTuple(21, 1)
            };
            Assert.Equal(resStart, await db.TimeSeriesRangeAsync(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));
                        
            // Aligh end
            var resEnd = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(0, 2),
                new TimeSeriesTuple(10, 1),
                new TimeSeriesTuple(20, 1)
            };
            Assert.Equal(resEnd, await db.TimeSeriesRangeAsync(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

            // Align 1
            Assert.Equal(resStart, await db.TimeSeriesRangeAsync(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
        }

        [Fact]
        public async Task TestMissingTimeBucket()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = await CreateData(db, key, 50);
            var ex = await Assert.ThrowsAsync<ArgumentException>(async () => await db.TimeSeriesRangeAsync(key, "-", "+", aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public async Task TestFilterBy()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var tuples = await CreateData(db, key, 50);

            var res = await db.TimeSeriesRangeAsync(key, "-", "+", filterByValue: (0,2)); // The first 3 tuples
            Assert.Equal(3, res.Count);
            Assert.Equal(tuples.GetRange(0,3), res);

            var filterTs = new List<TimeStamp> {0, 50, 100}; // Also the first 3 tuples
            res = await db.TimeSeriesRangeAsync(key, "-", "+", filterByTs: filterTs); 
            Assert.Equal(tuples.GetRange(0,3), res);

            res = await db.TimeSeriesRangeAsync(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5)); // The third tuple
            Assert.Equal(tuples.GetRange(2,1), res);
        }
    }
}
