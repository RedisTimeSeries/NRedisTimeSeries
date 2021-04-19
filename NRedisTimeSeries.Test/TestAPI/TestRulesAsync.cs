using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;
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
            var aggregations = (TsAggregation[])Enum.GetValues(typeof(TsAggregation));

            foreach (var aggregation in aggregations)
            {
                await db.TimeSeriesCreateAsync($"{key}:{aggregation}");
            }

            var timeBucket = 50L;
            var rules = new List<TimeSeriesRule>();
            var rulesMap = new Dictionary<TsAggregation, TimeSeriesRule>();
            foreach (var aggregation in aggregations)
            {
                var rule = new TimeSeriesRule($"{key}:{aggregation}", timeBucket, aggregation);
                rules.Add(rule);
                rulesMap[aggregation] = rule;
                Assert.True(await db.TimeSeriesCreateRuleAsync(key, rule));

                var info = await db.TimeSeriesInfoAsync(key);
                Assert.Equal(rules, info.Rules);
            }

            foreach (var aggregation in aggregations)
            {
                var rule = rulesMap[aggregation];
                rules.Remove(rule);
                Assert.True(await db.TimeSeriesDeleteRuleAsync(key, rule.DestKey));

                var info = await db.TimeSeriesInfoAsync(key);
                Assert.Equal(rules, info.Rules);
            }

            await db.KeyDeleteAsync(aggregations.Select(i => (RedisKey)$"{key}:{i}").ToArray());
        }

        [Fact]
        public async Task TestNonExistingSrc()
        {
            var key = CreateKeyName();
            var aggKey = $"{key}:{TsAggregation.Avg}";
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(aggKey);
            var rule = new TimeSeriesRule(aggKey, 50, TsAggregation.Avg);
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
            var aggKey = $"{key}:{TsAggregation.Avg}";
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(key);
            var rule = new TimeSeriesRule(aggKey, 50, TsAggregation.Avg);
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesCreateRuleAsync(key, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TimeSeriesDeleteRuleAsync(key, aggKey));
            Assert.Equal("ERR TSDB: compaction rule does not exist", ex.Message);
        }
    }
}
