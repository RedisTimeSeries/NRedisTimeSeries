using NRedisTimeSeries.DataTypes;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.Commands;
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
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key));
        }

        [Fact]
        public async Task TestCreateRetentionTime()
        {
            var key = CreateKeyName();
            long retentionTime = 5000;
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, retentionTime: retentionTime));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public async Task TestCreateLabels()
        {
            var key = CreateKeyName();
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
            var key = CreateKeyName();
            var labels = new List<TimeSeriesLabel>();
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, labels: labels));

            var info = await db.TimeSeriesInfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public async Task TestCreateUncompressed()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, uncompressed: true));
        }

        [Fact]
        public async void TestCreatehDuplicatePolicyFirst()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, duplicatePolicy: TsDuplicatePolicy.FIRST));
        }

        [Fact]
        public async void TestCreatehDuplicatePolicyLast()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, duplicatePolicy: TsDuplicatePolicy.LAST));
        }

        [Fact]
        public async void TestCreatehDuplicatePolicyMin()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, duplicatePolicy: TsDuplicatePolicy.MIN));
        }

        [Fact]
        public async void TestCreatehDuplicatePolicyMax()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, duplicatePolicy: TsDuplicatePolicy.MAX));
        }

        [Fact]
        public async void TestCreatehDuplicatePolicySum()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            Assert.True(await db.TimeSeriesCreateAsync(key, duplicatePolicy: TsDuplicatePolicy.SUM));
        }        
    }
}
