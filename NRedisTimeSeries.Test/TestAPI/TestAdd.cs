using System;
using System.Collections.Generic;
using System.Threading;
using NRedisTimeSeries.DataTypes;
using NRedisTimeSeries.Commands;
using StackExchange.Redis;
using Xunit;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestAdd : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "ADD_TESTS";

        public TestAdd(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestAddNotExistingTimeSeries()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
        }

        [Fact]
        public void TestAddExistingTimeSeries()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(key);
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
        }

        [Fact]
        public void TestAddStar()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesAdd(key, "*", 1.1);
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.True(info.FirstTimeStamp > 0);
            Assert.Equal(info.FirstTimeStamp, info.LastTimeStamp);
        }

        [Fact]
        public void TestAddWithRetentionTime()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            long retentionTime = 5000;
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1, retentionTime: retentionTime));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestAddWithLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1, labels: labels));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestAddWithUncompressed()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(key);
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1, uncompressed: true));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
        }
        
        [Fact]
        public void TestAddWithChunkSize()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1, chunkSizeBytes: 128));
            TimeSeriesInformation info = db.TimeSeriesInfo(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
            Assert.Equal(128, info.ChunkSize);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyBlock() 
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1));

            var ex = Assert.Throws<RedisServerException>(() => db.TimeSeriesAdd(key, timeStamp, 1.2));
            Assert.Equal("ERR TSDB: Error at upsert, update is not supported in BLOCK mode", ex.Message);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyMin()
        { 
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1));

            // Insert a bigger number and check that it did not change the value
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.2, policy: TsDuplicatePolicy.MIN));
            Assert.Equal(1.1, db.TimeSeriesRange(key, timeStamp, timeStamp)[0].Val);
            // Insert a smaller number and check that it changed
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.0, policy: TsDuplicatePolicy.MIN));
            Assert.Equal(1.0, db.TimeSeriesRange(key, timeStamp, timeStamp)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyMax()
        { 
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1));

            // Insert a smaller number and check that it did not change the value
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.0, policy: TsDuplicatePolicy.MAX));
            Assert.Equal(1.1, db.TimeSeriesRange(key, timeStamp, timeStamp)[0].Val);
            // Insert a bigger number and check that it changed
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.2, policy: TsDuplicatePolicy.MAX));
            Assert.Equal(1.2, db.TimeSeriesRange(key, timeStamp, timeStamp)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicySum()
        { 
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1));
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.0, policy: TsDuplicatePolicy.SUM));
            Assert.Equal(2.1, db.TimeSeriesRange(key, timeStamp, timeStamp)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyFirst()
        { 
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1));
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.0, policy: TsDuplicatePolicy.FIRST));
            Assert.Equal(1.1, db.TimeSeriesRange(key, timeStamp, timeStamp)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyLast()
        { 
            IDatabase db = redisFixture.Redis.GetDatabase();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.1));
            Assert.Equal(timeStamp, db.TimeSeriesAdd(key, timeStamp, 1.0, policy: TsDuplicatePolicy.LAST));
            Assert.Equal(1.0, db.TimeSeriesRange(key, timeStamp, timeStamp)[0].Val);
        }      

        [Fact]
        public void TestOldAdd()
        {
            TimeStamp old_dt = DateTime.UtcNow;
            Thread.Sleep(1000);
            TimeStamp new_dt = DateTime.UtcNow;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.TimeSeriesCreate(key);
            db.TimeSeriesAdd(key, new_dt, 1.1);
            // Adding old event
            Assert.Equal( old_dt, db.TimeSeriesAdd(key, old_dt, 1.1));
        }

        [Fact]
        public void TestWrongParameters()
        {
            double value = 1.1;
            IDatabase db = redisFixture.Redis.GetDatabase();
            var ex = Assert.Throws<RedisServerException>(() => db.TimeSeriesAdd(key, "+", value));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.TimeSeriesAdd(key, "-", value));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
        }
    }
}
