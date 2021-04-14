using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using NRedisTimeSeries.Extensions;

namespace NRedisTimeSeries
{
    public static partial class TimeSeriesClient
    {
        private static void AddRetentionTime(this IList<object> args, long? retentionTime)
        {
            if (retentionTime.HasValue)
            {
                args.Add(CommandArgs.RETENTION);
                args.Add(retentionTime);
            }
        }

        private static void AddChunkSize(this IList<object> args, long? chunkSize)
        {
            if (chunkSize.HasValue)
            {
                args.Add(CommandArgs.CHUNK_SIZE);
                args.Add(chunkSize);
            }
        }

        private static void AddLabels(this IList<object> args, IReadOnlyCollection<TimeSeriesLabel> labels)
        {
            if (labels != null)
            {
                args.Add(CommandArgs.LABELS);
                foreach (var label in labels)
                {
                    args.Add(label.Key);
                    args.Add(label.Value);
                }
            }
        }

        private static void AddUncompressed(this IList<object> args, bool? uncompressed)
        {
            if (uncompressed.HasValue)
            {
                args.Add(CommandArgs.UNCOMPRESSED);
            }
        }

        private static void AddCount(this IList<object> args, long? count)
        {
            if (count.HasValue)
            {
                args.Add(CommandArgs.COUNT);
                args.Add(count.Value);
            }
        }

        private static void AddDuplicatePolicy(this IList<object> args, TsDuplicatePolicy? policy)
        {
            if (policy.HasValue)
            {
                args.Add(CommandArgs.DUPLICATE_POLICY);
                args.Add(policy.Value.AsArg());
            }
        }

        
        private static void AddOnDuplicate(this IList<object> args, TsDuplicatePolicy? policy)
        {
            if (policy.HasValue)
            {
                args.Add(CommandArgs.ON_DUPLICATE);
                args.Add(policy.Value.AsArg());
            }
        }

        private static void AddAggregation(this IList<object> args, TsAggregation? aggregation, long? timeBucket)
        {
            if(aggregation != null)
            {
                args.Add(CommandArgs.AGGREGATION);
                args.Add(aggregation.Value.AsArg());
                if (!timeBucket.HasValue)
                {
                    throw new ArgumentException("RANGE Aggregation should have timeBucket value");
                }
                args.Add(timeBucket.Value);
            }
        }

        private static void AddFilters(this List<object> args, IReadOnlyCollection<string> filter)
        {
            if(filter == null || filter.Count == 0)
            {
                throw new ArgumentException("There should be at least one filter on MRANGE/MREVRANGE");
            }
            args.Add(CommandArgs.FILTER);
            foreach(string f in filter)
            {
                args.Add(f);
            }
        }

        private static void AddWithLabels(this IList<object> args, bool? withLabels)
        {
            if(withLabels.HasValue && withLabels.Value)
            {
                args.Add(CommandArgs.WITHLABELS);
            }
        }

        private static void AddTimeStamp(this IList<object> args, TimeStamp timeStamp)
        {
            if(timeStamp != null)
            {
                args.Add(CommandArgs.TIMESTAMP);
                args.Add(timeStamp.Value);
            }
        }

        private static void AddRule(this IList<object> args, TimeSeriesRule rule)
        {
            args.Add(rule.DestKey);
            args.Add(CommandArgs.AGGREGATION);
            args.Add(rule.Aggregation.AsArg());
            args.Add(rule.TimeBucket);
        }
        
        private static List<object> BuildTsCreateArgs(string key, long? retentionTime, IReadOnlyCollection<TimeSeriesLabel> labels, bool? uncompressed,
            long? chunkSizeBytes, TsDuplicatePolicy? policy)
        {
            var args = new List<object> {key};
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            args.AddDuplicatePolicy(policy);
            return args;
        }
        
        private static List<object> BuildTsAlterArgs(string key, long? retentionTime, IReadOnlyCollection<TimeSeriesLabel> labels)
        {
            var args = new List<object> {key};
            args.AddRetentionTime(retentionTime);
            args.AddLabels(labels);
            return args;
        }
        
        private static List<object> BuildTsAddArgs(string key, TimeStamp timestamp, double value, long? retentionTime,
            IReadOnlyCollection<TimeSeriesLabel> labels, bool? uncompressed, long? chunkSizeBytes, TsDuplicatePolicy? policy)
        {
            var args = new List<object> {key, timestamp.Value, value};
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            args.AddOnDuplicate(policy);
            return args;
        }
        
        private static List<object> BuildTsIncrDecrByArgs(string key, double value, TimeStamp timestamp, long? retentionTime,
            IReadOnlyCollection<TimeSeriesLabel> labels, bool? uncompressed, long? chunkSizeBytes)
        {
            var args = new List<object> {key, value};
            args.AddTimeStamp(timestamp);
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            return args;
        }

        private static List<object> BuildTsMaddArgs(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
        {
            var args = new List<object>();
            foreach (var tuple in sequence)
            {
                args.Add(tuple.key);
                args.Add(tuple.timestamp.Value);
                args.Add(tuple.value);
            }

            return args;
        }
        
        private static List<object> BuildTsMgetArgs(IReadOnlyCollection<string> filter, bool? withLabels)
        {
            var args = new List<object>();
            args.AddWithLabels(withLabels);
            AddFilters(args, filter);
            return args;
        }
        
        private static List<object> BuildRangeArgs(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp, long? count,
            TsAggregation? aggregation, long? timeBucket)
        {
            var args = new List<object>()
                {key, fromTimeStamp.Value, toTimeStamp.Value};
            args.AddCount(count);
            args.AddAggregation(aggregation, timeBucket);
            return args;
        }
        
        private static List<object> BuildMultiRangeArgs(TimeStamp fromTimeStamp, TimeStamp toTimeStamp, IReadOnlyCollection<string> filter,
            long? count, TsAggregation? aggregation, long? timeBucket, bool? withLabels)
        {
            var args = new List<object>() {fromTimeStamp.Value, toTimeStamp.Value};
            args.AddCount(count);
            args.AddAggregation(aggregation, timeBucket);
            args.AddWithLabels(withLabels);
            args.AddFilters(filter);
            return args;
        }
    }
}
