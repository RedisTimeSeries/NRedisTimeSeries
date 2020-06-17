using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for INFO and QUERYINDEX commands.
    /// </summary>
    internal class InfoQueryIndexExampleAsync
    {
        /// <summary>
        /// Example for getting the information of a timeseries key with INFO command.
        /// </summary>
        public static async Task InfoExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeSeriesInformation info = await db.TimeSeriesInfoAsync("my_ts");
            redis.Close();
        }

        /// <summary>
        /// Example for using QUERYINDEX.
        /// </summary>
        public static async Task QueryIndexExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "key=value" };
            IReadOnlyList<string> keys = await db.TimeSeriesQueryIndexAsync(filter);
            redis.Close();
        }
    }
}
