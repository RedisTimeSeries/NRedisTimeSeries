using System;
using System.Threading.Tasks;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries.Example
{
    /// <summary>
    /// Example for create and delete RedisTimeSeries rules.
    /// </summary>
    internal class RulesExampleAsync
    {
        /// <summary>
        /// Example for create and delete RedisTimeSeries rules.
        /// </summary>
        public static async Task RulesCreateDeleteExampleAsync()
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = redis.GetDatabase();
            // Create you rule with destination key, time bucket and aggregation type.
            TimeSeriesRule rule = new TimeSeriesRule("dest_ts", 50, Aggregation.AVG);
            await db.TimeSeriesCreateRuleAsync("my_ts", rule);
            await db.TimeSeriesDeleteRuleAsync("my_ts", "dest");
            redis.Close();
        }
    }
}
