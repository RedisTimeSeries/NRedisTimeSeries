﻿using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestMGet : AbstractTimeSeriesTest, IDisposable
    {

        private readonly string[] _keys = { "MGET_TESTS_1", "MGET_TESTS_2" };

        public TestMGet(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            foreach (string key in _keys)
            {
                redisFixture.Redis.GetDatabase().KeyDelete(key);
            }
        }

        [Fact]
        public void TestMGetQuery()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();

            var label1 = new TimeSeriesLabel("MGET_TESTS_1", "value");
            var label2 = new TimeSeriesLabel("MGET_TESTS_2", "value2");
            var labels1 = new List<TimeSeriesLabel> { label1, label2 };
            var labels2 = new List<TimeSeriesLabel> { label1 };

            TsTimeStamp ts1 = db.TimeSeriesAdd(_keys[0], 1.1, labels: labels1);
            TimeSeriesTuple tuple1 = new TimeSeriesTuple(ts1, 1.1);
            TsTimeStamp ts2 = db.TimeSeriesAdd(_keys[1], 2.2, labels: labels2);
            TimeSeriesTuple tuple2 = new TimeSeriesTuple(ts2, 2.2);
            var results = db.TimeSeriesMGet(new List<string> { "MGET_TESTS_1=value" });
            Assert.Equal(2, results.Count);
            Assert.Equal(_keys[0], results[0].key);
            Assert.Equal(tuple1, results[0].value);
            Assert.Equal(new List<TimeSeriesLabel>(), results[0].labels);
            Assert.Equal(_keys[1], results[1].key);
            Assert.Equal(tuple2, results[1].value);
            Assert.Equal(new List<TimeSeriesLabel>(), results[1].labels);

        }

        [Fact]
        public void TestMGetQueryWithLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();

            var label1 = new TimeSeriesLabel("MGET_TESTS_1", "value");
            var label2 = new TimeSeriesLabel("MGET_TESTS_2", "value2");
            var labels1 = new List<TimeSeriesLabel> { label1, label2 };
            var labels2 = new List<TimeSeriesLabel> { label1 };

            TsTimeStamp ts1 = db.TimeSeriesAdd(_keys[0], 1.1, labels: labels1);
            TimeSeriesTuple tuple1 = new TimeSeriesTuple(ts1, 1.1);
            TsTimeStamp ts2 = db.TimeSeriesAdd(_keys[1], 2.2, labels: labels2);
            TimeSeriesTuple tuple2 = new TimeSeriesTuple(ts2, 2.2);

            var results = db.TimeSeriesMGet(new List<string> { "MGET_TESTS_1=value" }, withLabels: true);
            Assert.Equal(2, results.Count);
            Assert.Equal(_keys[0], results[0].key);
            Assert.Equal(tuple1, results[0].value);
            Assert.Equal(labels1, results[0].labels);
            Assert.Equal(_keys[1], results[1].key);
            Assert.Equal(tuple2, results[1].value);
            Assert.Equal(labels2, results[1].labels);
        }
    }
}
