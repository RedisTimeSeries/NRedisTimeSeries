using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using Xunit;


namespace NRedisTimeSeries.Test.TestAPI
{
    public class TestDel : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "DEL_TESTS";

        public TestDel(RedisFixture redisFixture) : base(redisFixture) { }
        
        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        private List<TimeSeriesTuple> CreateData(IDatabase db, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (int i = 0; i < 10; i++)
            {
                TimeStamp ts = db.TimeSeriesAdd(key, i*timeBucket, i);
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        [Fact]
        public void TestDelNotExists()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var ex = Assert.Throws<RedisServerException>(()=>db.TimeSeriesDel(key, "-", "+"));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public void TestAddAndDel()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            var tuples = CreateData(db, 50);
            TimeStamp from = tuples[0].Time;
            TimeStamp to = tuples[5].Time;
            Assert.True(db.TimeSeriesDel(key, from, to));
            
            //check that the operation deleted the timestamps
            IReadOnlyList<TimeSeriesTuple> res = db.TimeSeriesRange(key, from, to);
            Assert.Equal(0, res.Count);
            Assert.NotNull(db.TimeSeriesGet(key));
        }
    }
}