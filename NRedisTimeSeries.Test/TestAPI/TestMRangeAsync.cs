﻿using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestMRangeAsync : AbstractTimeSeriesTest
    {
        public TestMRangeAsync(RedisFixture redisFixture) : base(redisFixture) { }

        private async Task<List<TimeSeriesTuple>> CreateData(IDatabase db, string[] keys, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();

            for (var i = 0; i < 10; i++)
            {
                var ts = new TimeStamp(i * timeBucket);
                foreach (var key in keys)
                {
                    await db.TimeSeriesAddAsync(key, ts, i);
                }
                tuples.Add(new TimeSeriesTuple(ts, i));
            }

            return tuples;
        }

        [Fact]
        public async Task TestSimpleMRange()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TimeSeriesCreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var results = db.TimeSeriesMRange("-", "+", new List<string> { $"{keys[0]}=value" });
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [Fact]
        public async Task TestMRangeWithLabels()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TimeSeriesCreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TimeSeriesMRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true);
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels, results[i].labels);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [Fact]
        public async Task TestMRangeSelectLabels()
        {
            var keys = CreateKeyNames(2);
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label1 = new TimeSeriesLabel(keys[0], "value");
            TimeSeriesLabel[] labels = new TimeSeriesLabel[]{ new TimeSeriesLabel("team", "CTO"), new TimeSeriesLabel("team", "AUT")};
            for (int i = 0; i < keys.Length; i++)
            {
                await db.TimeSeriesCreateAsync(keys[i], labels: new List<TimeSeriesLabel> { label1, labels[i] });
            }

            var tuples = await CreateData(db, keys, 50);
            // selectLabels and withlabels are mutualy exclusive. 
            var ex = await Assert.ThrowsAsync<ArgumentException>(async () => 
            {
                await db.TimeSeriesMRangeAsync("-", "+",  
                    new List<string> { "key=MRangeSelectLabels" },
                    withLabels: true, selectLabels: new List<string> {"team"});
            });
            Assert.Equal("withLabels and selectLabels cannot be specified together.", ex.Message);

            var results = await db.TimeSeriesMRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, selectLabels: new List<string> {"team"});
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels[i], results[i].labels[0]);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [Fact]
        public async Task TestMRangeFilter()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            await db.TimeSeriesCreateAsync(keys[0], labels: labels);
            var tuples = await CreateData(db, keys, 50);
            var results = await db.TimeSeriesMRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" });
            Assert.Equal(1, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(0, results[0].labels.Count);
            Assert.Equal(tuples, results[0].values);
        }

        [Fact]
        public async Task TestMRangeCount()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TimeSeriesCreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var count = 5L;
            var results = await db.TimeSeriesMRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, count: count);
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples.GetRange(0, (int)count), results[i].values);
            }
        }

        [Fact]
        public async Task TestMRangeAggregation()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TimeSeriesCreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TimeSeriesMRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, aggregation: TsAggregation.Min, timeBucket: 50);
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [Fact]
        public async Task TestMRangeAlign()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            db.TimeSeriesCreate(keys[0], labels: labels);
            await CreateData(db, keys, 50);
            var expected = new List<TimeSeriesTuple> {
                new TimeSeriesTuple(0,1),
                new TimeSeriesTuple(50,1),
                new TimeSeriesTuple(100,1)
            };
            var results = await db.TimeSeriesMRangeAsync(0, "+", new List<string> { $"{keys[0]}=value" }, align: "-", aggregation: TsAggregation.Count, timeBucket: 10, count:3);
            Assert.Equal(1, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(expected, results[0].values);
            results = await db.TimeSeriesMRangeAsync(0, 500, new List<string> { $"{keys[0]}=value" }, align: "+", aggregation: TsAggregation.Count, timeBucket: 10, count:1);
            Assert.Equal(expected[0], results[0].values[0]);
        }

        [Fact]
        public async Task TestMissingFilter()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TimeSeriesCreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var ex = await Assert.ThrowsAsync<ArgumentException>(async () => await db.TimeSeriesMRangeAsync("-", "+", new List<string>()));
            Assert.Equal("There should be at least one filter on MRANGE/MREVRANGE", ex.Message);
        }

        [Fact]
        public async Task TestMissingTimeBucket()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TimeSeriesCreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await db.TimeSeriesMRangeAsync("-", "+",
                    filter: new List<string>() { $"key=value" },
                    aggregation: TsAggregation.Avg);
            });
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public async Task TestMRangeGroupby()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            for(int i = 0; i < keys.Length; i++) 
            {
                var label1 = new TimeSeriesLabel(keys[0], "value");
                var label2 = new TimeSeriesLabel("group", i.ToString());
                await db.TimeSeriesCreateAsync(keys[i], labels: new List<TimeSeriesLabel> { label1, label2 });
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TimeSeriesMRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true, groupbyTuple: ("group", TsReduce.Min));
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal("group=" + i, results[i].key);
                Assert.Equal(new TimeSeriesLabel("group", i.ToString()), results[i].labels[0]);
                Assert.Equal(new TimeSeriesLabel("__reducer__", "min"), results[i].labels[1]);
                Assert.Equal(new TimeSeriesLabel("__source__", keys[i]), results[i].labels[2]);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [Fact]
        public async Task TestMRangeReduce()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            foreach(var key in keys)
            {
                var label = new TimeSeriesLabel(keys[0], "value");
                await db.TimeSeriesCreateAsync(key, labels: new List<TimeSeriesLabel> { label });
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TimeSeriesMRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true, groupbyTuple: (keys[0], TsReduce.Sum));
            Assert.Equal(1, results.Count);
            Assert.Equal($"{keys[0]}=value", results[0].key);
            Assert.Equal(new TimeSeriesLabel(keys[0], "value"), results[0].labels[0]);
            Assert.Equal(new TimeSeriesLabel("__reducer__", "sum"), results[0].labels[1]);
            Assert.Equal(new TimeSeriesLabel("__source__", string.Join(",", keys)), results[0].labels[2]);
            for(int i = 0; i < results[0].values.Count; i++)
            {
                Assert.Equal(tuples[i].Val * 2, results[0].values[i].Val);
            }
        }

        [Fact]
        public async Task TestMRangeFilterBy()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                await db.TimeSeriesCreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TimeSeriesMRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(tuples.GetRange(0,3), results[i].values);
            }

            results = await db.TimeSeriesMRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, filterByTs: new List<TimeStamp> {0}, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(tuples.GetRange(0,1), results[i].values);
            }
        }
    }
}
