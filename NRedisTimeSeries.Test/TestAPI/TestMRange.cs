using System;
using System.Collections.Generic;
using System.Threading;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.Commands.Enums;
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

        private List<TimeSeriesTuple> CreateData(IDatabase db, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();

            for (int i = 0; i < 10; i++)
            {
                TimeStamp ts = new TimeStamp(i*timeBucket);
                foreach (var key in keys)
                {
                    db.TimeSeriesAdd(key, ts, i);

                }
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        [Fact]
        public void TestSimpleMRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label = new TimeSeriesLabel("MRANGEkey", "MRANGEvalue");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TimeSeriesMRange("-", "+", new List<string>{ "MRANGEkey=MRANGEvalue" });
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
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeWithLabels");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TimeSeriesMRange("-", "+", new List<string> { "key=MRangeWithLabels" }, withLabels: true);
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
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeFilter");
            var labels = new List<TimeSeriesLabel> { label };
            db.TimeSeriesCreate(keys[0], labels: labels);
            var tuples = CreateData(db, 50);
            var results = db.TimeSeriesMRange("-", "+", new List<string> { "key=MRangeFilter" });
            Assert.Equal(1, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(0, results[0].labels.Count);
            Assert.Equal(tuples, results[0].values);
        }

        [Fact]
        public void TestMRangeCount()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeCount");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            long count = 5;
            var results = db.TimeSeriesMRange("-", "+", new List<string> { "key=MRangeCount" }, count:count);
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
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeAggregation");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TimeSeriesMRange("-", "+", new List<string> { "key=MRangeAggregation" }, aggregation: TsAggregation.Min, timeBucket: 50);
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [Fact]
        public void TestMissingFilter()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MissingFilter");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesMRange("-", "+", new List<string>()));
            Assert.Equal("There should be at least one filter on MRANGE/MREVRANGE", ex.Message);
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MissingTimeBucket");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesMRange("-", "+", new List<string> { "key=MissingTimeBucket" }, aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);

        }
    }
}
