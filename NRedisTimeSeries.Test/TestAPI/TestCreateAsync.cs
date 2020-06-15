using NRedisTimeSeries.DataTypes;
using NuGet.Frameworks;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestCreateAsync : AbstractTimeSeriesTest
    {
        public TestCreateAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestCreateOK()
        {
            var key = CreateKey();
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key));
        }

        [Fact]
        public async Task TestCreateRetentionTime()
        {
            var key = CreateKey();
            long retentionTime = 5000;
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, retentionTime: retentionTime));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public async Task TestCreateLabels()
        {
            var key = CreateKey();
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, labels: labels));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public async Task TestCreateEmptyLabels()
        {
            var key = CreateKey();
            var labels = new List<TimeSeriesLabel>();
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, labels: labels));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public async Task TestCreateUncompressed()
        {
            var key = CreateKey();
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, uncompressed: true));
        }
    }
}
