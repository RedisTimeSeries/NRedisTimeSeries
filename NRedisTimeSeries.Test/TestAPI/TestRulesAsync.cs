using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestRulesAsync : AbstractTimeSeriesTest
    {
        public TestRulesAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestRulesAdditionDeletion()
        {
            var key = CreateKey();
            var destKeys = CreateAggregateKeys();
            var db = redisFixture.Redis.GetDatabase();

            await db.TimeSeriesCreateAsync(key);
            foreach (var destKey in destKeys.Values)
            {
                await db.TimeSeriesCreateAsync(destKey);
            }

            var timeBucket = 50L;
            var rules = new List<TimeSeriesRule>();
            var rulesMap = new Dictionary<Aggregation, TimeSeriesRule>();
            foreach (var aggregation in destKeys.Keys)
            {
                var rule = new TimeSeriesRule(destKeys[aggregation], timeBucket, aggregation);
                rules.Add(rule);
                rulesMap[aggregation] = rule;
                Assert.True(await db.TimeSeriesCreateRuleAsync(key, rule));

                var info = await db.TimeSeriesInfoAsync(key);
                Assert.Equal(rules, info.Rules);
            }

            foreach (var aggregation in destKeys.Keys)
            {
                var rule = rulesMap[aggregation];
                rules.Remove(rule);
                Assert.True(await db.TimeSeriesDeleteRuleAsync(key, rule.DestKey));

                var info = await db.TimeSeriesInfoAsync(key);
                Assert.Equal(rules, info.Rules);
            }
        }

        [Fact]
        public async Task TestNonExistingSrc()
        {
            var key = CreateKey();
            var destKeys = CreateAggregateKeys();
            var db = redisFixture.Redis.GetDatabase();
            var destKey = destKeys[Aggregation.AVG];
            await db.TimeSeriesCreateAsync(destKey);
            var rule = new TimeSeriesRule(destKey, 50, Aggregation.AVG);
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesCreateRuleAsync(key, rule));
            Assert.Equal("TSDB: the key does not exist", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesDeleteRuleAsync(key, destKey));
            Assert.Equal("TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public async Task TestNonExisitingDestinaion()
        {
            var key = CreateKey();
            var destKeys = CreateAggregateKeys();
            var db = redisFixture.Redis.GetDatabase();
            var destKey = destKeys[Aggregation.AVG];
            await db.TimeSeriesCreateAsync(key);
            var rule = new TimeSeriesRule(destKey, 50, Aggregation.AVG);
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesCreateRuleAsync(key, rule));
            Assert.Equal("TSDB: the key does not exist", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesDeleteRuleAsync(key, destKey));
            Assert.Equal("TSDB: compaction rule does not exist", ex.Message);
        }
    }
}
