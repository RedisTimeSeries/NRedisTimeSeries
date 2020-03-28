using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for adding multiple samples to multiple time series.
    /// </summary>
    internal class MAddExample
    {
        /// <summary>
        /// Example for mutiple sample addtion. One is using RedisTimeSeris default system time in one time series,
        /// the second is using DateTime in the second time series and the third is using long in the third time series.
        /// </summary>
        public static void MAddFlowExample()
        {
            string[] keys = { "system_time_ts", "datetime_ts", "long_ts" };
            var sequence = new List<(string, TimeStamp, double)>(keys.Length);
            // Add sample to the system_time_ts
            sequence.Add((keys[0], "*", 0.0));
            // Add sample to the datetime_ts
            sequence.Add((keys[1], DateTime.Now, 0.0));
            // Add sample to the long_ts
            sequence.Add((keys[2], 1, 0.0));
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            db.TimeSeriesMAdd(sequence);
            redis.Close();
        }
    }
}
