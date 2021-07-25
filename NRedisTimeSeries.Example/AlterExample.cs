using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for altering time series properties.
    /// </summary>
    internal class AlterExample
    {
        /// <summary>
        /// Examples for altering time-series.
        /// The parameters retentionTime, and labels are optional and can be set in any order when used as named argument.
        /// </summary>
        public static void ParameterizedAlter()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            db.TimeSeriesAlter("labeld_ts", labels: labels);
            db.TimeSeriesAlter("retention_time_ts", retentionTime: 5000);
            db.TimeSeriesAlter("parameterized_ts", retentionTime: 5000, labels: labels);
            redis.Close();
        }
    }
}
