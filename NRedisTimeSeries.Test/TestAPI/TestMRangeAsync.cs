using NRedisTimeSeries.Commands;
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
            var results = await db.TimeSeriesMRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, aggregation: Aggregation.MIN, timeBucket: 50);
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples, results[i].values);
            }
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
            Assert.Equal("There should be at least one filter on MRANGE", ex.Message);
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
                    aggregation: Aggregation.AVG);
            });
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }
    }
}
