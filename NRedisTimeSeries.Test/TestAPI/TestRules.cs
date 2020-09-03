using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestRules : AbstractTimeSeriesTest, IDisposable
    {
        private string _srcKey = "RULES_TEST_SRC";

        private Dictionary<Aggregation, string> _destKeys;

        public TestRules(RedisFixture redisFixture) : base(redisFixture)
        {

            _destKeys = new Dictionary<Aggregation, string>
            {
                { Aggregation.AVG, "RULES_DEST_" + Aggregation.AVG },
                { Aggregation.COUNT, "RULES_DEST_" + Aggregation.COUNT },
                { Aggregation.FIRST, "RULES_DEST_" + Aggregation.FIRST },
                { Aggregation.LAST, "RULES_DEST_" + Aggregation.LAST },
                { Aggregation.MAX, "RULES_DEST_" + Aggregation.MAX },
                { Aggregation.MIN, "RULES_DEST_" + Aggregation.MIN },
                { Aggregation.RANGE, "RULES_DEST_" + Aggregation.RANGE },
                { Aggregation.STDP, "RULES_DEST_" + Aggregation.STDP },
                { Aggregation.STDS, "RULES_DEST_" + Aggregation.STDS },
                { Aggregation.SUM, "RULES_DEST_" + Aggregation.SUM },
                { Aggregation.VARP, "RULES_DEST_" + Aggregation.VARP },
                { Aggregation.VARS, "RULES_DEST_" + Aggregation.VARS }
            };
        }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(_srcKey);
            foreach(var key in _destKeys.Values)
            {
                redisFixture.Redis.GetDatabase().KeyDelete(key);
            }
        }

        [Fact]
        public void TestRulesAdditionDeletion()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(_srcKey);
            foreach(var destKey in _destKeys.Values)
            {
                db.TimeSeriesCreate(destKey);
            }
            long timeBucket = 50;
            var rules = new List<TimeSeriesRule>();
            var rulesMap = new Dictionary<Aggregation, TimeSeriesRule>();
            foreach(var aggregation in _destKeys.Keys)
            {
                var rule = new TimeSeriesRule(_destKeys[aggregation], timeBucket, aggregation);
                rules.Add(rule);
                rulesMap[aggregation] = rule;
                Assert.True(db.TimeSeriesCreateRule(_srcKey, rule));
                TimeSeriesInformation info = db.TimeSeriesInfo(_srcKey);
                Assert.Equal(rules, info.Rules);
            }
            foreach(var aggregation in _destKeys.Keys)
            {
                var rule = rulesMap[aggregation];
                rules.Remove(rule);
                Assert.True(db.TimeSeriesDeleteRule(_srcKey, rule.DestKey));
                TimeSeriesInformation info = db.TimeSeriesInfo(_srcKey);
                Assert.Equal(rules, info.Rules);
            }
        }

        [Fact]
        public void TestNonExistingSrc()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            string destKey = "RULES_DEST_" + Aggregation.AVG;
            db.TimeSeriesCreate(destKey);
            TimeSeriesRule rule = new TimeSeriesRule(destKey, 50, Aggregation.AVG);
            var ex = Assert.Throws<RedisServerException>(() => db.TimeSeriesCreateRule(_srcKey, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.TimeSeriesDeleteRule(_srcKey, destKey));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public void TestNonExisitingDestinaion()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            string destKey = "RULES_DEST_" + Aggregation.AVG;
            db.TimeSeriesCreate(_srcKey);
            TimeSeriesRule rule = new TimeSeriesRule(destKey, 50, Aggregation.AVG);
            var ex = Assert.Throws<RedisServerException>(() => db.TimeSeriesCreateRule(_srcKey, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.TimeSeriesDeleteRule(_srcKey, destKey));
            Assert.Equal("ERR TSDB: compaction rule does not exist", ex.Message);
        }
    }
}
