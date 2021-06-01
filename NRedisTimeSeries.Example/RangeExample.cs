using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for RANGE queries.
    /// </summary>
    public class RangeExample
    {
        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis.
        /// NRedisTimeSeris Range is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesRange command returns an IReadOnlyList<TimeSeriesTuple> collection.
        /// </summary>
        public static void DefaultRangeExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            IReadOnlyList<TimeSeriesTuple> results = db.TimeSeriesRange("my_ts", "-", "+");
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis, and the COUNT parameter.
        /// NRedisTimeSeris Range is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesRange command returns an IReadOnlyList<TimeSeriesTuple> collection.
        /// </summary>
        public static void CountRangeExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            IReadOnlyList<TimeSeriesTuple> results = db.TimeSeriesRange("my_ts", "-", "+", count:50);
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis, and MIN aggregation.
        /// NRedisTimeSeris Range is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesRange command returns an IReadOnlyList<TimeSeriesTuple> collection.
        /// </summary>
        public static void RangeAggregationExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            db.TimeSeriesRange("my_ts", "-", "+", aggregation: TsAggregation.Min, timeBucket: 50);
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with "-" and "+" as range boundreis, FILTER_BY_TS, and FILTER_BY_VALUE.
        /// NRedisTimeSeris Range is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesRange command returns an IReadOnlyList<TimeSeriesTuple> collection.
        /// </summary>
        public static void RangeFilterByExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filterTs = new List<TimeStamp> {0, 50, 100};
            IReadOnlyList<TimeSeriesTuple> results = db.TimeSeriesRange("my_ts", "-", "+", filterByTs: filterTs, filterByValue: (2, 5));
            foreach(TimeSeriesTuple res in results) {
                Console.WriteLine(res);
            }
            redis.Close();
        }        
    }
}
