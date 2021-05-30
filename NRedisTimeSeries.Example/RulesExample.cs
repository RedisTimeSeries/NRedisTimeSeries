using System;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Example for create and delete RedisTimeSeries rules.
    /// </summary>
    internal class RulesExample
    {
        /// <summary>
        /// Example for create and delete RedisTimeSeries rules.
        /// </summary>
        public static void  RulesCreateDeleteExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            // Create new timeseries for the sourceKey and the destKey.
            db.TimeSeriesCreate("my_ts");
            db.TimeSeriesCreate("dest_ts");
            // Create your rule with destination key, time bucket and aggregation type.
            TimeSeriesRule rule = new TimeSeriesRule("dest_ts", 50, TsAggregation.Avg);
            db.TimeSeriesCreateRule("my_ts", rule);
            db.TimeSeriesDeleteRule("my_ts", "dest_ts");
            redis.Close();
        }
    }
}
