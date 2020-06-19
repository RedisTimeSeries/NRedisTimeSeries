using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for INFO and QUERYINDEX commands.
    /// </summary>
    internal class InfoQueryIndexAsyncExample
    {
        /// <summary>
        /// Example for getting the information of a timeseries key with INFO command.
        /// </summary>
        public static async Task InfoAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeSeriesInformation info = await db.TimeSeriesInfoAsync("my_ts");
            redis.Close();
        }

        /// <summary>
        /// Example for using QUERYINDEX.
        /// </summary>
        public static async Task QueryIndexAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            var filter = new List<string> { "key=value" };
            IReadOnlyList<string> keys = await db.TimeSeriesQueryIndexAsync(filter);
            redis.Close();
        }
    }
}
