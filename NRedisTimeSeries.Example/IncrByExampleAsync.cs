using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries async API for INCRBY.
    /// </summary>
    internal class IncrByAsyncExample
    {
        /// <summary>
        /// Example for increasing the value of the last sample by 5. 
        /// </summary>
        public static async Task DefaultIncrByAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesIncrByAsync("my_ts", 5);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to DateTime.UtcNow and its value to 5, with INCRBY. 
        /// </summary>
        public static async Task DateTimeIncrByAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesIncrByAsync("my_ts", 5, timestamp: DateTime.UtcNow);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to long value and its value to 5, with INCRBY. 
        /// </summary>
        public static async Task LongIncrByAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesIncrByAsync("my_ts", 5, timestamp: 1000);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to system time and its value to 5, with INCRBY.
        /// The parameters retentionTime, uncompressed and labels are optional and can be set in any order when used as named argument.
        /// </summary>
        public static async Task ParameterizedIncrByAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            await db.TimeSeriesIncrByAsync("my_ts", 5, retentionTime: 5000, uncompressed: true, labels: labels);
            redis.Close();
        }
    }
}
