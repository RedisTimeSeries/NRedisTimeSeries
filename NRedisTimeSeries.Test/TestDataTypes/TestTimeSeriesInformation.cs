using System;
using System.Collections.Generic;
using StackExchange.Redis;
using NRedisTimeSeries.DataTypes;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.Test.TestAPI;
using Xunit;

namespace NRedisTimeSeries.Test.TestDataTypes
{
    public class TestInformation : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "INFORMATION_TESTS";

        public TestInformation(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestInformationToString()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesAdd(key, "*", 1.1);
            db.TimeSeriesAdd(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            string[] infoProperties = ((string)info).Trim('{').Trim('}').Split(",");
            Assert.Equal("\"TotalSamples\":2", infoProperties[0]);
            Assert.Equal("\"MemoryUsage\":4184", infoProperties[1]);
            Assert.Equal("\"RetentionTime\":0", infoProperties[4]);
            Assert.Equal("\"ChunkCount\":1", infoProperties[5]);
            Assert.Equal("\"DuplicatePolicy\":null", infoProperties[11]);
        }
    }
}
