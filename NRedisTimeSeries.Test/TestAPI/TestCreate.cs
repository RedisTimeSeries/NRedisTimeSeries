using System;
using NRedisTimeSeries;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestCreate : IDisposable, IClassFixture<RedisFixture>
    {
        private RedisFixture redisFixture;

        private readonly string key = "CREATE_ts1";

        public TestCreate(RedisFixture redisFixture) => this.redisFixture = redisFixture;

        public void Dispose()
        {
            redisFixture.redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestCreateOK()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
        }

        [Fact]
        public void TestCreateRetentionTime()
        {
            long retentionTime = 5000;
            IDatabase db = redisFixture.redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(key, retentionTime: retentionTime));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }
    }
}
