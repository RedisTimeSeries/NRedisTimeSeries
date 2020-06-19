using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System.Threading.Tasks;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries async API for GET queries.
    /// </summary>
    internal class GetAsyncExample
    {
        /// <summary>
        /// Example for GET query. The NRedisTimeSeries TimeSeriesGet command returns a TimeSeriesTuple object.
        /// </summary>
        public static async Task SimpleGetAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeSeriesTuple value = await db.TimeSeriesGetAsync("my_ts");
            redis.Close();
        }
    }
}
