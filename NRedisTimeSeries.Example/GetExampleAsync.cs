using System;
using System.Threading.Tasks;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for GET queries.
    /// </summary>
    internal class GetExampleAsync
    {
        /// <summary>
        /// Example for GET query. The NRedisTimeSeries TimeSeriesGet command returns a TimeSeriesTuple object.
        /// </summary>
        public static async Task  SimpleGetExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeSeriesTuple value = await db.TimeSeriesGetAsync("my_ts");
            redis.Close();
        }
    }
}
