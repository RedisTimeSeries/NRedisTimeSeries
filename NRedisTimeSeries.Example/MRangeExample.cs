using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for MRANGE queries.
    /// </summary>
    internal class MRangeExample
    {
        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with TsTimeStamp.MinValue and TsTimeStamp.MaxValue as range boundreis and a filter.
        /// NRedisTimeSeris MRange is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesMRange command returns an IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>collection.
        /// </summary>
        public static void  BasicMRangeExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "MRANGEkey=MRANGEvalue" };
            var results = db.TimeSeriesMRange(TsTimeStamp.MinValue, TsTimeStamp.MaxValue, filter);
            // Values extraction example. No lables in this case.
            foreach (var result in results)
            {
                string key = result.key;
                IReadOnlyList<TimeSeriesTuple> values = result.values;
            }
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with TsTimeStamp.MinValue and TsTimeStamp.MaxValue as range boundreis, a filter and the COUNT parameter.
        /// NRedisTimeSeris MRange is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesMRange command returns an IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>collection.
        /// </summary>
        public static void CountMRangeExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "MRANGEkey=MRANGEvalue" };
            var results = db.TimeSeriesMRange(TsTimeStamp.MinValue, TsTimeStamp.MaxValue, filter, count:50);
            // Values extraction example. No lables in this case.
            foreach (var result in results)
            {
                string key = result.key;
                IReadOnlyList<TimeSeriesTuple> values = result.values;
            }
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with TsTimeStamp.MinValue and TsTimeStamp.MaxValue as range boundreis, a filter and MIN aggregation.
        /// NRedisTimeSeris MRange is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesMRange command returns an IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>collection.
        /// </summary>
        public static void MRangeAggregationExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "MRANGEkey=MRANGEvalue" };
            var results = db.TimeSeriesMRange(TsTimeStamp.MinValue, TsTimeStamp.MaxValue, filter, aggregation:Aggregation.MIN, timeBucket:50);
            // Values extraction example. No lables in this case.
            foreach (var result in results)
            {
                string key = result.key;
                IReadOnlyList<TimeSeriesTuple> values = result.values;
            }
            redis.Close();
        }

        /// <summary>
        /// Example for basic usage of RedisTimeSeries RANGE command with TsTimeStamp.MinValue and TsTimeStamp.MaxValue as range boundreis, a filter and WITHLABELS flag.
        /// NRedisTimeSeris MRange is expecting two TimeStamps objects as the range boundries.
        /// In this case, the strings are implicitly casted into TimeStamp objects.
        /// The TimeSeriesMRange command returns an IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>collection.
        /// </summary>
        public static void MRangeWithLabelsExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "MRANGEkey=MRANGEvalue" };
            var results = db.TimeSeriesMRange(TsTimeStamp.MinValue, TsTimeStamp.MaxValue, filter,withLabels:true);
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
