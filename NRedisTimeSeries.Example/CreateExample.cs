using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System.Collections.Generic;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for creating new time series.
    /// </summary>
    internal class CreateExample
    {
        /// <summary>
        /// Simple time-series creation.
        /// </summary>
        public static void SimpleCreate()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            db.TimeSeriesCreate("my_ts");
            redis.Close();
        }

        /// <summary>
        /// Examples for creating time-series with parameters.
        /// The parameters retentionTime, uncompressed and labels are optional and can be set in any order when used as named argument.
        /// </summary>
        public static void ParameterizedCreate()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            db.TimeSeriesCreate("retentionTime_ts", retentionTime: 5000);
            db.TimeSeriesCreate("uncompressed_ts", uncompressed: true);
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            db.TimeSeriesCreate("labeled_ts", labels: labels);
            db.TimeSeriesCreate("parameterized_ts", labels: labels, uncompressed: true, retentionTime: 5000);
            redis.Close();
        }
    }
}
