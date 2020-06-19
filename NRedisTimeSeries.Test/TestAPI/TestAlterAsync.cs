using NRedisTimeSeries.DataTypes;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestAlterAsync : AbstractTimeSeriesTest
    {
        public TestAlterAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestAlterRetentionTime()
        {
            var key = CreateKeyName();
            long retentionTime = 5000;
            var db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesCreateAsync(key);
            Assert.True(await db.TimeSeriesAlterAsync(key, retentionTime: retentionTime));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public async Task TestAlterLabels()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            await db.TimeSeriesCreateAsync(key);
            Assert.True(await db.TimeSeriesAlterAsync(key, labels: labels));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(labels, info.Labels);

            labels.Clear();
            Assert.True(await db.TimeSeriesAlterAsync(key, labels: labels));

            info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

    }
}
