using StackExchange.Redis;
using NRedisTimeSeries.DataTypes;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.Test.TestAPI;
using System.Threading.Tasks;
using Xunit;

namespace NRedisTimeSeries.Test.TestDataTypes
{
    public class TestInformation : AbstractTimeSeriesTest
    {
        public TestInformation(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestInformationToStringAsync()
        {
            string key = CreateKeyName();
            IDatabase db = redisFixture.Redis.GetDatabase();
            await db.TimeSeriesAddAsync(key, "*", 1.1);
            await db.TimeSeriesAddAsync(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);
            TimeSeriesInformation info = await db.TimeSeriesInfoAsync(key);
            string[] infoProperties = ((string)info).Trim('{').Trim('}').Split(",");
            Assert.Equal("\"TotalSamples\":2", infoProperties[0]);
            Assert.Equal("\"MemoryUsage\":4184", infoProperties[1]);
            Assert.Equal("\"RetentionTime\":0", infoProperties[4]);
            Assert.Equal("\"ChunkCount\":1", infoProperties[5]);
            Assert.Equal("\"DuplicatePolicy\":null", infoProperties[11]);
        }
    }
}
