using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for altering time series properties.
    /// </summary>
    internal class AlterExampleAsync
    {
        /// <summary>
        /// Examples for altering time-series.
        /// The parameters retentionTime, and labels are optional and can be set in any order when used as named argument.
        /// </summary>
        public static async Task ParameterizedAlterAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            await db.TimeSeriesAlterAsync("labeld_ts", labels: labels);
            await db.TimeSeriesAlterAsync("retention_time_ts", retentionTime: 5000);
            await db.TimeSeriesAlterAsync("parameterized_ts", retentionTime: 5000, labels: labels);
            redis.Close();
        }
    }
}
