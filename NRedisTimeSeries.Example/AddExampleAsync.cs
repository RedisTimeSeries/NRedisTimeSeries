using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries async API for adding new sample to time series.
    /// </summary>
    internal class AddAsyncExample
    {
        /// <summary>
        /// Example for using RedisTimeSeries default "*" charecter for system time.
        /// The TimeSeriesAdd method gets a TimeStamp type parameter, which in this case the string "*"
        /// is implicitly casted into a new TimeStamp object.
        /// </summary>
        public static async Task DefaultAddAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeStamp timestamp = "*";
            await db.TimeSeriesAddAsync("my_ts", timestamp, 0.0);
            redis.Close();
        }

        /// <summary>
        /// Example for using TimeStamp as long value.
        /// The TimeSeriesAdd method gets a TimeStamp type parameter, which in this case the value 1
        /// is implicitly casted into a new TimeStamp object.
        /// </summary>
        public static async Task LongAddAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeStamp timestamp = 1;
            await db.TimeSeriesAddAsync("my_ts", timestamp, 0.0);
            redis.Close();
        }

        /// <summary>
        /// Example for using TimeStamp as DateTime value.
        /// The TimeSeriesAdd method gets a TimeStamp type parameter, which in this case the value DateTime.UtcNow
        /// is implicitly casted into a new TimeStamp object.
        /// </summary>
        public static async Task DateTimeAddAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeStamp timestamp = DateTime.UtcNow;
            await db.TimeSeriesAddAsync("my_ts", timestamp, 0.0);
            redis.Close();
        }

        /// <summary>
        /// Example for back-fill time-series sample.
        /// In order to avoid the BLOCK mode exeption must specify the duplicate policy.
        /// </summary>
        public static async void DuplicatedAddAsync()
        {
            
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeStamp timestamp = DateTime.UtcNow;
            await db.TimeSeriesAddAsync("my_ts", timestamp, 1);
            await db.TimeSeriesAddAsync("my_ts", timestamp, 0, duplicatePolicy: TsDuplicatePolicy.MIN);
            redis.Close();
        }

        /// <summary>
        /// Example for time-series creation parameters with ADD.
        /// Named arguments are used in the same manner of TimeSeriesCreate.
        /// </summary>
        public static async Task ParameterizedAddAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeStamp timestamp = "*";
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            await db.TimeSeriesAddAsync("my_ts", timestamp, 0.0, retentionTime:5000, labels:labels, uncompressed:true);
            redis.Close();
        }
    }
}
