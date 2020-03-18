using System;
using StackExchange.Redis;

namespace NRedisTimeSeries
{
    public static partial class TimeSeriesClient
    {
        private static bool ParseBoolean(RedisResult result)
        {
            return (string)result == "OK";            
        }

        private static TimeStamp ParseTimeStamp(RedisResult result)
        {
            if (result.Type == ResultType.None) return null;
            return new TimeStamp((long)result);
        }

        private static Value ParseValue(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            if (redisResults.Length == 0) return null;
            return new Value(ParseTimeStamp(redisResults[0]), (double)redisResults[1]);
        }
    }
}
