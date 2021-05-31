using System;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries API for GET queries.
    /// </summary>
    internal class GetExample
    {
        /// <summary>
        /// Example for GET query. The NRedisTimeSeries TimeSeriesGet command returns a TimeSeriesTuple object.
        /// </summary>
        public static void  SimpleGetExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            TimeSeriesTuple value = db.TimeSeriesGet("my_ts");
            Console.WriteLine(value.ToString());
            redis.Close();
        }
    }
}
