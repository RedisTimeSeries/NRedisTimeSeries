using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestMRevRange : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string[] keys = { "MREVRANGE_TESTS_1", "MREVRANGE_TESTS_2" };

        public TestMRevRange(RedisFixture redisFixture) : base(redisFixture) { }

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
        public void TestSimpleMRevRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label = new TimeSeriesLabel("MREVRANGEkey", "MREVRANGEvalue");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string>{ "MREVRANGEkey=MREVRANGEvalue" });
            Assert.Equal(keys.Length, results.Count);
            for(int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public void TestMRevRangeWithLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRevRangeWithLabels");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { "key=MRevRangeWithLabels" }, withLabels: true);
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels, results[i].labels);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public void TestMRevRangeFilter()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRevRangeFilter");
            var labels = new List<TimeSeriesLabel> { label };
            db.TimeSeriesCreate(keys[0], labels: labels);
            var tuples = CreateData(db, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { "key=MRevRangeFilter" });
            Assert.Equal(1, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(0, results[0].labels.Count);
            Assert.Equal(ReverseData(tuples), results[0].values);
        }

        [Fact]
        public void TestMRevRangeCount()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRevRangeCount");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            long count = 5;
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { "key=MRevRangeCount" }, count:count);
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(ReverseData(tuples).GetRange(0, (int)count), results[i].values);
            }
        }

        [Fact]
        public void TestMRevRangeAggregation()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRevRangeAggregation");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { "key=MRevRangeAggregation" }, aggregation: Aggregation.MIN, timeBucket: 50);
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(ReverseData(tuples), results[i].values);
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
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesMRevRange("-", "+", new List<string>()));
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
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesMRevRange("-", "+", new List<string> { "key=MissingTimeBucket" }, aggregation: Aggregation.AVG));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);

        }
    }
}
