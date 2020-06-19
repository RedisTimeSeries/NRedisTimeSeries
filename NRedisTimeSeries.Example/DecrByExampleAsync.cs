using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries async API for DECRBY.
    /// </summary>
    internal class DecrByAsyncExample
    {
        /// <summary>
        /// Example for decreasing the value of the last sample by 5. 
        /// </summary>
        public static async Task DefaultDecrByAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesDecrByAsync("my_ts", 5);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to system time and its value to -5, with DECRBY. 
        /// </summary>
        public static async Task SystemTimeDecrByAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesDecrByAsync("my_ts", 5, timestamp: "*");
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to DateTime.UtcNow and its value to -5, with DECRBY. 
        /// </summary>
        public static async Task DateTimeDecrByAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesDecrByAsync("my_ts", 5, timestamp: DateTime.UtcNow);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to long value and its value to -5, with DECRBY. 
        /// </summary>
        public static async Task LongDecrByAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesDecrByAsync("my_ts", 5, timestamp: long.MaxValue);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to system time and its value to -5, with DECRBY.
        /// The parameters retentionTime, uncompressed and labels are optional and can be set in any order when used as named argument.
        /// </summary>
        public static async Task ParameterizedDecrByAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            await db.TimeSeriesDecrByAsync("my_ts", 5, timestamp: "*", retentionTime: 5000, uncompressed: true, labels: labels);
            redis.Close();
        }
    }
}
