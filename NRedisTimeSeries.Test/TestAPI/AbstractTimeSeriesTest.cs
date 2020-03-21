using System;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public abstract class AbstractTimeSeriesTest: IClassFixture<RedisFixture>
    {
        protected internal RedisFixture redisFixture;

        protected internal AbstractTimeSeriesTest(RedisFixture redisFixture) => this.redisFixture = redisFixture;

    }
}
