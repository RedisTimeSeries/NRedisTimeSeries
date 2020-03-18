using System;
using StackExchange.Redis;

namespace NRedisTimeSeries.Test.TestAPI
{
    public class RedisFixture : IDisposable
    {
        public RedisFixture() => redis = ConnectionMultiplexer.Connect("localhost");

        public void Dispose()
        {
            redis.Close();
        }

        public ConnectionMultiplexer redis { get; private set; }
    }
}
