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
        /// Example for mutiple sample addtion. One is using default TsTimeStamp value in one time series,
        /// the second is using current system DateTime in the second time series and the third is using a specified TsTimeStamp in the third time series.
        /// </summary>
        public static void MAddWithTimeStampExample()
        {
            string[] keys = { "ts_first", "ts_second", "ts_third" };
            var sequence = new List<(string, TsTimeStamp, double)>(keys.Length)
            {
                (keys[0], default(TsTimeStamp), 1.0),
                (keys[1], DateTime.UtcNow, 1.0),
                (keys[2], new TsTimeStamp(1), 1.0)
            };
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            db.TimeSeriesMAdd(sequence);
            redis.Close();
        }

        /// <summary>
        /// Example for mutiple sample addtion using the Redis system timestamp. Two samples are added to the first timeseries,
        /// and two samples are added to the second timeseries.
        /// </summary>
        public static void MAddWithoutTimeStampExample()
        {
            string[] keys = { "ts_first", "ts_second" };
            var sequence = new List<(string, double)>(keys.Length)
            {
                (keys[0], 1.0),
                (keys[0], 2.0),
                (keys[1], 1.0),
                (keys[1], 2.0)
            };
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            db.TimeSeriesMAdd(sequence);
            redis.Close();
        }
    }
}
