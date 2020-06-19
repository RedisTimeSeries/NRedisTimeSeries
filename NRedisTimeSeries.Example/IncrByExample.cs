using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for INCRBY.
    /// </summary>
    internal class IncrByExample
    {
        /// <summary>
        /// Example for increasing the value of the last sample by 5. 
        /// </summary>
        public static void DefaultIncrByExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            db.TimeSeriesIncrBy("my_ts", 5);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to system time and its value to 5, with INCRBY. 
        /// </summary>
        public static void SystemTimeIncrByExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            db.TimeSeriesIncrBy("my_ts", 5, timestamp: "*");
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to DateTime.UtcNow and its value to 5, with INCRBY. 
        /// </summary>
        public static void DateTimeIncrByExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            db.TimeSeriesIncrBy("my_ts", 5, timestamp: DateTime.UtcNow);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to long value and its value to 5, with INCRBY. 
        /// </summary>
        public static void LongIncrByExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            db.TimeSeriesIncrBy("my_ts", 5, timestamp: long.MaxValue);
            redis.Close();
        }

        /// <summary>
        /// Example for setting the last sample timestamp to system time and its value to 5, with INCRBY.
        /// The parameters retentionTime, uncompressed and labels are optional and can be set in any order when used as named argument.
        /// </summary>
        public static void ParameterizedIncrByExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            db.TimeSeriesIncrBy("my_ts", 5, timestamp: "*", retentionTime:5000, uncompressed:true, labels: labels);
            redis.Close();
        }
    }
}
