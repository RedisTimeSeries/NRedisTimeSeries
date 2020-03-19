using System;
using NRedisTimeSeries;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestCreate : IDisposable, IClassFixture<RedisFixture>
    {
        private RedisFixture redisFixture;

        private readonly string keyname = "CREATE_ts1";

        public TestCreate(RedisFixture redisFixture) => this.redisFixture = redisFixture;

        public void Dispose()
        {
            redisFixture.redis.GetDatabase().KeyDelete(keyname);
        }

        [Fact]
        public void TestCreateOK()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(keyname));
            db.KeyDelete(keyname);
        }

        [Fact]
        public void TestCreateRetentionTime()
        {
            IDatabase db = redisFixture.redis.GetDatabase();
            Assert.True(db.TimeSeriesCreate(keyname, retentionTime: 5000));
            db.KeyDelete(keyname);

        }
    }
}
