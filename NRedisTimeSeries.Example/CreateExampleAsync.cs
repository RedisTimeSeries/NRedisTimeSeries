using NRedisTimeSeries.DataTypes;
using NRedisTimeSeries.Commands.Enums;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries async API for creating new time series.
    /// </summary>
    internal class CreateAsyncExample
    {
        /// <summary>
        /// Simple time-series creation.
        /// </summary>
        public static async Task SimpleCreateAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesCreateAsync("my_ts");
            redis.Close();
        }

        /// <summary>
        /// Examples for creating time-series with parameters.
        /// The parameters retentionTime, uncompressed and labels are optional and can be set in any order when used as named argument.
        /// </summary>
        public static async Task ParameterizedCreateAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesCreateAsync("retentionTime_ts", retentionTime: 5000);
            await db.TimeSeriesCreateAsync("uncompressed_ts", uncompressed: true);
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            await db.TimeSeriesCreateAsync("labeled_ts", labels: labels);
            await db.TimeSeriesCreateAsync("parameterized_ts", labels: labels, uncompressed: true, retentionTime: 5000, duplicatePolicy: TsDuplicatePolicy.LAST);
            redis.Close();
        }
    }
}
