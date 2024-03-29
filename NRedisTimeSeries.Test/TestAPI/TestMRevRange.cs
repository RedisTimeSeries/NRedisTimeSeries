﻿using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestMRevRange : AbstractTimeSeriesTest
    {
        public TestMRevRange(RedisFixture redisFixture) : base(redisFixture) { }

        private List<TimeSeriesTuple> CreateData(IDatabase db, string[] keys, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();

            for (var i = 0; i < 10; i++)
            {
                var ts = new TimeStamp(i*timeBucket);
                foreach (var key in keys)
                {
                    db.TimeSeriesAdd(key, ts, i);

                }
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        [Fact]
        public void TestSimpleMRevRange()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, keys, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string>{ $"{keys[0]}=value" });
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public void TestMRevRangeWithLabels()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, keys, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true);

            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels, results[i].labels);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public void TestMRevRangeSelectLabels()
        {
            var keys = CreateKeyNames(2);
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeSeriesLabel label1 = new TimeSeriesLabel("key", "MRangeSelectLabels");
            TimeSeriesLabel[] labels = new TimeSeriesLabel[]{ new TimeSeriesLabel("team", "CTO"), new TimeSeriesLabel("team", "AUT")};
            for (int i = 0; i < keys.Length; i++)
            {
                db.TimeSeriesCreate(keys[i], labels: new List<TimeSeriesLabel> { label1, labels[i] });
            }

            var tuples = CreateData(db, keys, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { "key=MRangeSelectLabels" }, selectLabels: new List<string> {"team"});
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels[i], results[i].labels[0]);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public void TestMRevRangeFilter()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            db.TimeSeriesCreate(keys[0], labels: labels);
            var tuples = CreateData(db, keys, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { $"{keys[0]}=value" });
            Assert.Equal(1, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(0, results[0].labels.Count);
            Assert.Equal(ReverseData(tuples), results[0].values);
        }

        [Fact]
        public void TestMRevRangeCount()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, keys, 50);
            var count = 5L;
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { $"{keys[0]}=value" }, count: count);
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(ReverseData(tuples).GetRange(0, (int)count), results[i].values);
            }
        }

        [Fact]
        public void TestMRevRangeAggregation()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, keys, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { $"{keys[0]}=value" }, aggregation: TsAggregation.Min, timeBucket: 50);
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public void TestMRevRangeAlign()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            db.TimeSeriesCreate(keys[0], labels: labels);
            CreateData(db, keys, 50);
            var expected = new List<TimeSeriesTuple> {
                new TimeSeriesTuple(450,1),               
                new TimeSeriesTuple(400,1),
                new TimeSeriesTuple(350,1)
            };
            var results = db.TimeSeriesMRevRange(0, "+", new List<string> { $"{keys[0]}=value" }, align: "-", aggregation: TsAggregation.Count, timeBucket: 10, count:3);
            Assert.Equal(1, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(expected, results[0].values);
            results = db.TimeSeriesMRevRange(0, 500, new List<string> { $"{keys[0]}=value" }, align: "+", aggregation: TsAggregation.Count, timeBucket: 10, count:1);
            Assert.Equal(expected[0], results[0].values[0]);
        }

        [Fact]
        public void TestMissingFilter()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, keys, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesMRevRange("-", "+", new List<string>()));
            Assert.Equal("There should be at least one filter on MRANGE/MREVRANGE", ex.Message);
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, keys, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TimeSeriesMRevRange("-", "+", new List<string> { "key=MissingTimeBucket" }, aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public void TestMRevRangeGroupby()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            for(int i = 0; i < keys.Length; i++) 
            {
                var label1 = new TimeSeriesLabel(keys[0], "value");
                var label2 = new TimeSeriesLabel("group", i.ToString());
                db.TimeSeriesCreate(keys[i], labels: new List<TimeSeriesLabel> { label1, label2 });
            }

            var tuples = CreateData(db, keys, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true, groupbyTuple: ("group", TsReduce.Min));
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal("group=" + i, results[i].key);
                Assert.Equal(new TimeSeriesLabel("group", i.ToString()), results[i].labels[0]);
                Assert.Equal(new TimeSeriesLabel("__reducer__", "min"), results[i].labels[1]);
                Assert.Equal(new TimeSeriesLabel("__source__", keys[i]), results[i].labels[2]);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public void TestMRevRangeReduce()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            foreach(var key in keys)
            {
                var label = new TimeSeriesLabel(keys[0], "value");
                db.TimeSeriesCreate(key, labels: new List<TimeSeriesLabel> { label });
            }

            var tuples = CreateData(db, keys, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true, groupbyTuple: (keys[0], TsReduce.Sum));
            Assert.Equal(1, results.Count);
            Assert.Equal($"{keys[0]}=value", results[0].key);
            Assert.Equal(new TimeSeriesLabel(keys[0], "value"), results[0].labels[0]);
            Assert.Equal(new TimeSeriesLabel("__reducer__", "sum"), results[0].labels[1]);
            Assert.Equal(new TimeSeriesLabel("__source__", string.Join(",", keys)), results[0].labels[2]);
            tuples = ReverseData(tuples);
            for(int i = 0; i < results[0].values.Count; i++)
            {
                Assert.Equal(tuples[i].Val * 2, results[0].values[i].Val);
            }
        }

        [Fact]
        public void TestMRevRangeFilterBy()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TimeSeriesCreate(key, labels: labels);
            }

            var tuples = CreateData(db, keys, 50);
            var results = db.TimeSeriesMRevRange("-", "+", new List<string> { "key=MRangeFilterBy" }, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(ReverseData(tuples.GetRange(0,3)), results[i].values);
            }

            results = db.TimeSeriesMRevRange("-", "+", new List<string> { "key=MRangeFilterBy" }, filterByTs: new List<TimeStamp> {0}, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(ReverseData(tuples.GetRange(0,1)), results[i].values);
            }
        }        
    }
}
