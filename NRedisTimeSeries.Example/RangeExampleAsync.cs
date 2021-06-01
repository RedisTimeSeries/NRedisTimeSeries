using NRedisTimeSeries.Commands;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries async API for RANGE queries.
    /// </summary>
    public class RangeAsyncExample
    {
        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis.
        /// NRedisTimeSeris Range is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesRange command returns an IReadOnlyList<TimeSeriesTuple> collection.
        /// </summary>
        public static async Task DefaultRangeAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            IReadOnlyList<TimeSeriesTuple> results = await db.TimeSeriesRangeAsync("my_ts", "-", "+");
            foreach(TimeSeriesTuple res in results) {
                Console.WriteLine(res);
            }            
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis, and the COUNT parameter.
        /// NRedisTimeSeris Range is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesRange command returns an IReadOnlyList<TimeSeriesTuple> collection.
        /// </summary>
        public static async Task CountRangeAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            IReadOnlyList<TimeSeriesTuple> results = await db.TimeSeriesRangeAsync("my_ts", "-", "+", count:50);
            foreach(TimeSeriesTuple res in results) {
                Console.WriteLine(res);
            }            
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis, and MIN aggregation.
        /// NRedisTimeSeris Range is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesRange command returns an IReadOnlyList<TimeSeriesTuple> collection.
        /// </summary>
        public static async Task RangeAggregationAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            IReadOnlyList<TimeSeriesTuple> results = await db.TimeSeriesRangeAsync("my_ts", "-", "+", aggregation: TsAggregation.Min, timeBucket: 50);
            foreach(TimeSeriesTuple res in results) {
                Console.WriteLine(res);
            }            
            redis.Close();
        }
    }
}
