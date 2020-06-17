using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for INCRBY.
    /// </summary>
    internal class IncrByExampleAsync
    {
        /// <summary>
        /// Example for increasing the value of the last sample by 5. 
        /// </summary>
        public static async Task DefaultIncrByExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesIncrByAsync("my_ts", 5);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to system time and its value to 5, with INCRBY. 
        /// </summary>
        public static async Task SystemTimeIncrByExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesIncrByAsync("my_ts", 5, timestamp: "*");
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to DateTime.Now and its value to 5, with INCRBY. 
        /// </summary>
        public static async Task DateTimeIncrByExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesIncrByAsync("my_ts", 5, timestamp: DateTime.Now);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to long value and its value to 5, with INCRBY. 
        /// </summary>
        public static async Task LongIncrByExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesIncrByAsync("my_ts", 5, timestamp: long.MaxValue);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to system time and its value to 5, with INCRBY.
        /// The parameters retentionTime, uncompressed and labels are optional and can be set in any order when used as named argument.
        /// </summary>
        public static async Task ParameterizedIncrByExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            await db.TimeSeriesIncrByAsync("my_ts", 5, timestamp: "*", retentionTime: 5000, uncompressed: true, labels: labels);
            redis.Close();
        }
    }
}
