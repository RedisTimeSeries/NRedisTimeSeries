using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Linq;
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
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(key);

            foreach (var aggregation in Aggregation.GetEnumerator())
            {
                await db.TimeSeriesCreateAsync($"{key}:{aggregation.Name}");
            }

            var timeBucket = 50L;
            var rules = new List<TimeSeriesRule>();
            var rulesMap = new Dictionary<Aggregation, TimeSeriesRule>();
            foreach (var aggregation in Aggregation.GetEnumerator())
            {
                var rule = new TimeSeriesRule($"{key}:{aggregation.Name}", timeBucket, aggregation);
                rules.Add(rule);
                rulesMap[aggregation] = rule;
                Assert.True(await db.TimeSeriesCreateRuleAsync(key, rule));

                var info = await db.TimeSeriesInfoAsync(key);
                Assert.Equal(rules, info.Rules);
            }

            foreach (var aggregation in Aggregation.GetEnumerator())
            {
                var rule = rulesMap[aggregation];
                rules.Remove(rule);
                Assert.True(await db.TimeSeriesDeleteRuleAsync(key, rule.DestKey));

                var info = await db.TimeSeriesInfoAsync(key);
                Assert.Equal(rules, info.Rules);
            }

            await db.KeyDeleteAsync(Aggregation.GetEnumerator().Select(i => (RedisKey)$"{key}:{i.Name}").ToArray());
        }

        [Fact]
        public async Task TestNonExistingSrc()
        {
            var key = CreateKeyName();
            var aggKey = $"{key}:{Aggregation.AVG.Name}";
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(aggKey);
            var rule = new TimeSeriesRule(aggKey, 50, Aggregation.AVG);
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesCreateRuleAsync(key, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesDeleteRuleAsync(key, aggKey));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);

            await db.KeyDeleteAsync(aggKey);
        }

        [Fact]
        public async Task TestNonExisitingDestinaion()
        {
            var key = CreateKeyName();
            var aggKey = $"{key}:{Aggregation.AVG.Name}";
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(key);
            var rule = new TimeSeriesRule(aggKey, 50, Aggregation.AVG);
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesCreateRuleAsync(key, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesDeleteRuleAsync(key, aggKey));
            Assert.Equal("ERR TSDB: compaction rule does not exist", ex.Message);
        }
    }
}
