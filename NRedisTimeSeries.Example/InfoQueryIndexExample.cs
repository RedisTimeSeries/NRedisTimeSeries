using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for INFO and QUERYINDEX commands.
    /// </summary>
    internal class InfoQueryIndexExample
    {
        /// <summary>
        /// Example for getting the information of a timeseries key with INFO command.
        /// </summary>
        public static void InfoExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeSeriesInformation info = db.TimeSeriesInfo("my_ts");
            redis.Close();
        }

        /// <summary>
        /// Example for using QUERYINDEX.
        /// </summary>
        public static void QueryIndexExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "key=value" };
            IReadOnlyList<string> keys = db.TimeSeriesQueryIndex(filter);
            redis.Close();
        }
    }
}
