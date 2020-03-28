using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
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

        private static IReadOnlyList<TimeStamp> ParseTimeStampArray(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<TimeStamp>(redisResults.Length);
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, timestamp => list.Add(ParseTimeStamp(timestamp)));
            return list;
        }

        private static TimeSeriesTuple ParseTimeSeriesTuple(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            if (redisResults.Length == 0) return null;
            return new TimeSeriesTuple(ParseTimeStamp(redisResults[0]), (double)redisResults[1]);
        }

        private static IReadOnlyList<TimeSeriesTuple> ParseTimeSeriesTupleArray(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<TimeSeriesTuple>(redisResults.Length);
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, tuple => list.Add(ParseTimeSeriesTuple(tuple)));
            return list;
        }

        private static IReadOnlyList<TimeSeriesLabel> ParseLabelArray(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<TimeSeriesLabel>(redisResults.Length);
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, labelResult =>
            {
                RedisResult[] labelTuple = (RedisResult[])labelResult;
                list.Add(new TimeSeriesLabel((string)labelTuple[0], (string)labelTuple[1]));
            });
            return list;
        }

        private static IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> ParseMGetesponse(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple values)>(redisResults.Length);
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, MRangeValue =>
            {
                RedisResult[] MRangeTuple = (RedisResult[])MRangeValue;
                string key = (string)MRangeTuple[0];
                IReadOnlyList<TimeSeriesLabel> labels = ParseLabelArray(MRangeTuple[1]);
                TimeSeriesTuple value = ParseTimeSeriesTuple(MRangeTuple[2]);
                list.Add((key, labels, value));
            });
            return list;
        }

        private static IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> ParseMRangeResponse(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>(redisResults.Length);
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, MRangeValue =>
            {
                RedisResult[] MRangeTuple = (RedisResult[])MRangeValue;
                string key = (string)MRangeTuple[0];
                IReadOnlyList<TimeSeriesLabel> labels = ParseLabelArray(MRangeTuple[1]);
                IReadOnlyList<TimeSeriesTuple> values = ParseTimeSeriesTupleArray(MRangeTuple[2]);
                list.Add((key, labels, values));
            });
            return list;
        }

        private static TimeSeriesRule ParseRule(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            string destKey = (string)redisResults[0];
            long bucketTime = (long)redisResults[1];
            Aggregation aggregation = (string)redisResults[2];
            return new TimeSeriesRule(destKey, bucketTime, aggregation);
        }

        private static IReadOnlyList<TimeSeriesRule> ParseRuleArray(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<TimeSeriesRule>();
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, rule => list.Add(ParseRule(rule)));
            return list;
        }

        private static TimeSeriesInformation ParseInfo(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            long totalSamples = (long)redisResults[1];
            long memoryUsage = (long)redisResults[3];
            TimeStamp firstTimeStamp = ParseTimeStamp(redisResults[5]);
            TimeStamp lastTimeStamp = ParseTimeStamp(redisResults[7]);
            long retentionTime = (long)redisResults[9];
            long chunkCount = (long)redisResults[11];
            long maxSamplesPerChunk = (long)redisResults[13];
            IReadOnlyList<TimeSeriesLabel> labels = ParseLabelArray(redisResults[15]);
            string destKey = (string)redisResults[17];
            IReadOnlyList <TimeSeriesRule> rules = ParseRuleArray(redisResults[19]);
            return new TimeSeriesInformation(totalSamples, memoryUsage, firstTimeStamp, lastTimeStamp, retentionTime, chunkCount, maxSamplesPerChunk, labels, destKey, rules);
        }

        private static IReadOnlyList<string> ParseStringArray(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<string>();
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, str => list.Add((string)str));
            return list;
        }
    }
}