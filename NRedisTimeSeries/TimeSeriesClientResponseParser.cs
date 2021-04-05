using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using NRedisTimeSeries.Extensions;
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
            var aggregation = AggregationExtensions.AsAggregation((string)redisResults[2]);
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

        private static TsPolicy ParsePolicy(RedisResult result)
        {
            return PolicyExtensions.AsPolicy((string)result);
        }

        private static TimeSeriesInformation ParseInfo(RedisResult result)
        {
            long totalSamples = -1, memoryUsage = -1, retentionTime = -1, chunkSize=-1, chunkCount = -1;
            TimeStamp firstTimestamp = null, lastTimestamp = null;
            IReadOnlyList<TimeSeriesLabel> labels = null;
            IReadOnlyList <TimeSeriesRule> rules = null;
            TsPolicy policy = TsPolicy.BLOCK;
            string sourceKey = null;
            RedisResult[] redisResults = (RedisResult[])result;
            for(int i=0; i<redisResults.Length ; ++i){
                string label = (string)redisResults[i++];
                switch (label) {
                    case "totalSamples":
                        totalSamples = (long)redisResults[i];
                        break;
                    case "memoryUsage":
                        memoryUsage = (long)redisResults[i];
                        break;
                    case "retentionTime":
                        retentionTime = (long)redisResults[i];
                        break;
                    case "chunkCount":
                        chunkCount = (long)redisResults[i];
                        break;
                    case "chunkSize":
                        chunkSize = (long)redisResults[i];
                        break;
                    case "maxSamplesPerChunk":
                        // If the property name is maxSamplesPerChunk then this is an old
                        // version of RedisTimeSeries and we used the number of samples before ( now Bytes )
                        chunkSize = chunkSize * 16;
                        break;
                    case "firstTimestamp":
                        firstTimestamp = ParseTimeStamp(redisResults[i]);
                        break;
                    case "lastTimestamp":
                        lastTimestamp = ParseTimeStamp(redisResults[i]);
                        break;
                    case "labels":
                        labels = ParseLabelArray(redisResults[i]);
                        break;
                    case "sourceKey":
                        sourceKey = (string)redisResults[i];
                        break;
                    case "rules":
                        rules = ParseRuleArray(redisResults[i]);
                        break;
                    case "duplicatePolicy":
                        policy = ParsePolicy(redisResults[i]);
                        break;
                }
            }

            return new TimeSeriesInformation(totalSamples, memoryUsage, firstTimestamp,
            lastTimestamp, retentionTime, chunkCount, chunkSize, labels, sourceKey, rules, policy);
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
