using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries async API for MRANGE queries.
    /// </summary>
    internal class MRangeAsyncExample
    {
        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis and a filter.
        /// NRedisTimeSeris MRange is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesMRange command returns an IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>collection.
        /// </summary>
        public static async Task BasicMRangeAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "MRANGEkey=MRANGEvalue" };
            var results = await db.TimeSeriesMRangeAsync("-", "+", filter);
            // Values extraction example. No lables in this case.
            foreach (var result in results)
            {
                string key = result.key;
                IReadOnlyList<TimeSeriesTuple> values = result.values;
            }
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis, a filter and the COUNT parameter.
        /// NRedisTimeSeris MRange is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesMRange command returns an IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>collection.
        /// </summary>
        public static async Task CountMRangeAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "MRANGEkey=MRANGEvalue" };
            var results = await db.TimeSeriesMRangeAsync("-", "+", filter, count: 50);
            // Values extraction example. No lables in this case.
            foreach (var result in results)
            {
                string key = result.key;
                IReadOnlyList<TimeSeriesTuple> values = result.values;
            }
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis, a filter and MIN aggregation.
        /// NRedisTimeSeris MRange is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesMRange command returns an IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>collection.
        /// </summary>
        public static async Task MRangeAggregationAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "MRANGEkey=MRANGEvalue" };
            var results = await db.TimeSeriesMRangeAsync("-", "+", filter, aggregation: TsAggregation.Min, timeBucket: 50);
            // Values extraction example. No lables in this case.
            foreach (var result in results)
            {
                string key = result.key;
                IReadOnlyList<TimeSeriesTuple> values = result.values;
            }
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis, a filter and WITHLABELS flag.
        /// NRedisTimeSeris MRange is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesMRange command returns an IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>collection.
        /// </summary>
        public static async Task MRangeWithLabelsAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "MRANGEkey=MRANGEvalue" };
            var results = await db.TimeSeriesMRangeAsync("-", "+", filter, withLabels: true);
            // Values extraction example.
            foreach (var result in results)
            {
                string key = result.key;
                IReadOnlyList<TimeSeriesLabel> labels = result.labels;
                IReadOnlyList<TimeSeriesTuple> values = result.values;
            }
            redis.Close();
        }
    }
}
