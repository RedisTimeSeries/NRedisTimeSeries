using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System.Threading.Tasks;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Examples for NRedisTimeSeries async API creating and deleting rules.
    /// </summary>
    internal class RulesAsyncExample
    {
        /// <summary>
        /// Example for create and delete RedisTimeSeries rules.
        /// </summary>
        public static async Task RulesCreateDeleteAsyncExample()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            // Create your rule with destination key, time bucket and aggregation type.
            TimeSeriesRule rule = new TimeSeriesRule("dest_ts", 50, Aggregation.AVG);
            await db.TimeSeriesCreateRuleAsync("my_ts", rule);
            await db.TimeSeriesDeleteRuleAsync("my_ts", "dest");
            redis.Close();
        }
    }
}
