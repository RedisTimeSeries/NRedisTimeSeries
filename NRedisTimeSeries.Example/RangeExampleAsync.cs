using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for RANGE queries.
    /// </summary>
    public class RangeExampleAsync
    {
        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis.
        /// NRedisTimeSeris Range is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesRange command returns an IReadOnlyList<TimeSeriesTuple> collection.
        /// </summary>
        public static async Task DefaultRangeExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            IReadOnlyList<TimeSeriesTuple> results = await db.TimeSeriesRangeAsync("my_ts", "-", "+");
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis, and the COUNT parameter.
        /// NRedisTimeSeris Range is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesRange command returns an IReadOnlyList<TimeSeriesTuple> collection.
        /// </summary>
        public static async Task CountRangeExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            IReadOnlyList<TimeSeriesTuple> results = await db.TimeSeriesRangeAsync("my_ts", "-", "+", count:50);
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis, and MIN aggregation.
        /// NRedisTimeSeris Range is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesRange command returns an IReadOnlyList<TimeSeriesTuple> collection.
        /// </summary>
        public static async Task RangeAggregationExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            await db.TimeSeriesRangeAsync("my_ts", "-", "+", aggregation: Aggregation.MIN, timeBucket: 50);
            redis.Close();
        }
    }
}
