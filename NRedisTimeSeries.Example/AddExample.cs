﻿using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
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
            db.TimeSeriesAdd("my_ts", 0.0);
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
            var timestamp = new TsTimeStamp(1);
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
            var timestamp = DateTime.UtcNow;
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
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            db.TimeSeriesAdd("my_ts", 0.0, retentionTime: 5000, labels: labels, uncompressed: true);
            redis.Close();
        }
    }
}
