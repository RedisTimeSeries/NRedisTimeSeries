using NRedisTimeSeries.Commands;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public abstract class AbstractTimeSeriesTest : IClassFixture<RedisFixture>, IAsyncLifetime
    {
        protected internal RedisFixture redisFixture;

        protected internal AbstractTimeSeriesTest(RedisFixture redisFixture) => this.redisFixture = redisFixture;

        private List<string> dbKeys = new List<string>();

        protected internal string CreateKey([CallerMemberName] string memberName = "") => CreateKeys(1, memberName)[0];

        protected internal string[] CreateKeys(int count, [CallerMemberName] string memberName = "")
        {
            if (count < 1) throw new ArgumentOutOfRangeException(nameof(count), "Must be greater than zero.");

            var newKeys = new string[count];
            for (var i = 0; i < count; i++)
            {
                newKeys[i] = $"{GetType().Name}:{memberName}:{i}";
            }

            dbKeys.AddRange(newKeys);

            return newKeys;
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            var redis = redisFixture.Redis.GetDatabase();
            foreach (var key in dbKeys)
            {
                await redis.KeyDeleteAsync(key);
            }

            dbKeys.Clear();
            dbKeys = null;
        }

        protected Dictionary<Aggregation, string> CreateAggregateKeys([CallerMemberName] string memberName = "")
        {
            var keys = CreateKeys(12);
            var aggKeys = new Dictionary<Aggregation, string>
            {
                { Aggregation.AVG, keys[0] },
                { Aggregation.COUNT, keys[1] },
                { Aggregation.FIRST, keys[2] },
                { Aggregation.LAST, keys[3] },
                { Aggregation.MAX, keys[4] },
                { Aggregation.MIN, keys[5] },
                { Aggregation.RANGE, keys[6] },
                { Aggregation.STDP, keys[7] },
                { Aggregation.STDS, keys[8] },
                { Aggregation.SUM, keys[9] },
                { Aggregation.VARP, keys[10] },
                { Aggregation.VARS, keys[11] }
            };

            return aggKeys;
        }
    }
}
