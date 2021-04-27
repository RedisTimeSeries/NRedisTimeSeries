using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.Commands.Enums;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for adding new sample to time series.
    /// </summary>
    internal class AddExample
    {
        /// <summary>
        /// Example for using RedisTimeSeries default "*" charecter for system time.
        /// The TimeSeriesAdd method gets a TimeStamp type parameter, which in this case the string "*"
        /// is implicitly casted into a new TimeStamp object.
        /// </summary>
        public static void DefaultAdd()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeStamp timestamp = "*";
            db.TimeSeriesAdd("my_ts", timestamp, 0.0);
            redis.Close();
        }

        /// <summary>
        /// Example for using TimeStamp as long value.
        /// The TimeSeriesAdd method gets a TimeStamp type parameter, which in this case the value 1
        /// is implicitly casted into a new TimeStamp object.
        /// </summary>
        public static void LongAdd()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeStamp timestamp = 1;
            db.TimeSeriesAdd("my_ts", timestamp, 0.0);
            redis.Close();
        }

        /// <summary>
        /// Example for using TimeStamp as DateTime value.
        /// The TimeSeriesAdd method gets a TimeStamp type parameter, which in this case the value DateTime.UtcNow
        /// is implicitly casted into a new TimeStamp object.
        /// </summary>
        public static void DateTimeAdd()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeStamp timestamp = DateTime.UtcNow;
            db.TimeSeriesAdd("my_ts", timestamp, 0.0);
            redis.Close();
        }

        /// <summary>
        /// Example for time-series creation parameters with ADD.
        /// Named arguments are used in the same manner of TimeSeriesCreate
        /// </summary>
        public static void ParameterizedAdd()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeStamp timestamp = "*";
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            var policy = TsDuplicatePolicy.SUM;
            db.TimeSeriesAdd("my_ts", timestamp, 0.0, retentionTime:5000, labels:labels, uncompressed:true, policy:policy);
            redis.Close();
        }
    }
}
