using System;
using System.Collections.Generic;
using System.Threading;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestMRange : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string[] keys = { "MRANGE_TESTS_1", "MRANGE_TESTS_2" };

        public TestMRange(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            foreach (string key in keys)
            {
                redisFixture.Redis.GetDatabase().KeyDelete(key);
            }
        }

        private List<TimeSeriesTuple> CreateData(IDatabase db, int sleeptime)
        {
            var tuples = new List<TimeSeriesTuple>();

            for (int i = 0; i < 10; i++)
            {
                TimeStamp ts = DateTime.Now;
                foreach (var key in keys)
                {
                    db.TimeSeriesAdd(key, ts, i);

                }
                tuples.Add(new TimeSeriesTuple(ts, i));
                Thread.Sleep(sleeptime);
            }
            return tuples;
        }

        [Fact]
        public void TestSimpleMRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel labal = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { labal };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TimeSeriesMRange("-", "+", new List<string>{ "key=value"});
            Assert.Equal(keys.Length, results.Count);
            for(int i =0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples, results[i].values);
            } 
        }

        [Fact]
        public void TestMRangeWithLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel labal = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { labal };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TimeSeriesMRange("-", "+", new List<string> { "key=value" }, withLabels: true);
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels, results[i].labels);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [Fact]
        public void TestMRangeFilter()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel labal = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { labal };
            db.TimeSeriesCreate(keys[0], labels: labels);
            var tuples = CreateData(db, 50);
            var results = db.TimeSeriesMRange("-", "+", new List<string> { "key=value" });
            Assert.Equal(1, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(0, results[0].labels.Count);
            Assert.Equal(tuples, results[0].values);
        }

        [Fact]
        public void TestMRangeCount()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel labal = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { labal };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            long count = 5;
            var results = db.TimeSeriesMRange("-", "+", new List<string> { "key=value" }, count:count);
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples.GetRange(0,(int)count), results[i].values);
            }
        }

        [Fact]
        public void TestMRangeAggregation()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel labal = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { labal };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            Aggregation[] aggregarions = { Aggregation.AVG, Aggregation.COUNT, Aggregation.FIRST, Aggregation.LAST, Aggregation.MAX, Aggregation.MIN, Aggregation.RANGE, Aggregation.STDP, Aggregation.STDS, Aggregation.SUM, Aggregation.VARP, Aggregation.VARS };
            Array.ForEach(aggregarions, aggregation =>
            {
                var results = db.TimeSeriesMRange("-", "+", new List<string> { "key=value" }, aggregation: aggregation, timeBucket: 50);
                Assert.Equal(keys.Length, results.Count);
                for (int i = 0; i < results.Count; i++)
                {
                    Assert.Equal(keys[i], results[i].key);
                    Assert.Equal(0, results[i].labels.Count);
                    Assert.Equal(tuples, results[i].values);
                }
            });
        }

        [Fact]
        public void TestMissingFilter()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel labal = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { labal };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesMRange("-", "+", new List<string>()));
            Assert.Equal("There should be at least one filter on MRANGE", ex.Message);
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel labal = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { labal };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesMRange("-", "+", new List<string> { "key=value" }, aggregation: Aggregation.AVG));
            Assert.Equal("RAGNE Aggregation should have timeBucket value", ex.Message);

        }
    }
}
