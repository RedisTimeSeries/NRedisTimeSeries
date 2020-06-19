using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries async API for MGET queries.
    /// </summary>
    internal class MGetAsyncExample
    {
        /// <summary>
        /// Example for basic usage of RedisTimeSeries MGET command with a filter.
        /// The NRedisTimeSeries SimpleMGetExample returns and IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> collection.
        /// </summary>
        public static async Task SimpleMGetAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "key=value" };
            var results = await db.TimeSeriesMGetAsync(filter);
            // Values extraction example. No lables in this case.
            foreach (var result in results)
            {
                string key = result.key;
                TimeSeriesTuple value = result.value;
            }
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries MGET command with a filter and WITHLABELS flag.
        /// The NRedisTimeSeries SimpleMGetExample returns and IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> collection.
        /// </summary>
        public static async Task MGetWithLabelsAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "key=value" };
            var results = await db.TimeSeriesMGetAsync(filter, withLabels: true);
            // Values extraction example.
            foreach (var result in results)
            {
                string key = result.key;
                IReadOnlyList<TimeSeriesLabel> labels = result.labels;
                TimeSeriesTuple value = result.value;
            }
            redis.Close();
        }
    }
}
