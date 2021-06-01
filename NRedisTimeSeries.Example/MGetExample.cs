using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for MGET queries.
    /// </summary>
    internal class MGetExample
    {
        /// <summary>
        /// Example for basic usage of RedisTimeSeries MGET command with a filter.
        /// The NRedisTimeSeries SimpleMGetExample returns and IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> collection.
        /// </summary>
        public static void SimpleMGetExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "key=value" };
            var results = db.TimeSeriesMGet(filter);
            // Values extraction example. No lables in this case.
            foreach (var result in results)
            {
                Console.WriteLine(result.key);
                TimeSeriesTuple value = result.value;
                Console.WriteLine(value);
            }
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries MGET command with a filter and WITHLABELS flag.
        /// The NRedisTimeSeries SimpleMGetExample returns and IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> collection.
        /// </summary>
        public static void MGetWithLabelsExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "key=value" };
            var results = db.TimeSeriesMGet(filter, withLabels: true);
            // Values extraction example.
            foreach (var result in results)
            {
                Console.WriteLine(result.key);
                IReadOnlyList<TimeSeriesLabel> labels = result.labels;
                foreach(TimeSeriesLabel label in labels){
                    Console.WriteLine(label);
                }
                TimeSeriesTuple value = result.value;
                Console.WriteLine(value);
            }
            redis.Close();
        }
    }
}
