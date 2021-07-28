using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.Commands.Enums;
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

        private static void AddAlign(this IList<object> args, TimeStamp align)
        {
            if(align != null) 
            {
                args.Add(CommandArgs.ALIGN);
                args.Add(align.Value);
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

        private static void AddFilterByTs(this List<object> args, IReadOnlyCollection<TimeStamp> filter)
        {
            if (filter != null) 
            {
                args.Add(CommandArgs.FILTER_BY_TS);
                foreach (var ts in filter)
                {
                    args.Add(ts.Value);
                }
            }
        }

        private static void AddFilterByValue(this List<object> args, (long, long)? filter)
        {
            if (filter != null) 
            {
                args.Add(CommandArgs.FILTER_BY_VALUE);
                args.Add(filter.Value.Item1);
                args.Add(filter.Value.Item2);
            }
        }

        private static void AddWithLabels(this IList<object> args, bool? withLabels, IReadOnlyCollection<string> selectLabels = null)
        {   
            if(withLabels.HasValue && selectLabels != null) {
                throw new ArgumentException("withLabels and selectLabels cannot be specified together.");
            }

            if(withLabels.HasValue && withLabels.Value)
            {
                args.Add(CommandArgs.WITHLABELS);
            }

            if(selectLabels != null){
                args.Add(CommandArgs.SELECTEDLABELS);
                foreach(string label in selectLabels){
                    args.Add(label);
                }
            }
        }

        private static void AddGroupby(this IList<object> args, (string groupby, TsReduce reduce)? groupbyTuple) 
        {
            if (groupbyTuple.HasValue) 
            {
                args.Add(CommandArgs.GROPUBY);
                args.Add(groupbyTuple.Value.groupby);
                args.Add(CommandArgs.REDUCE);
                args.Add(groupbyTuple.Value.reduce.AsArg());
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

        private static List<object> BuildTsDelArgs(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
        {
            var args = new List<object> 
                {key, fromTimeStamp.Value, toTimeStamp.Value};
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
            args.AddFilters(filter);
            return args;
        }
        
        private static List<object> BuildRangeArgs(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp, long? count,
            TsAggregation? aggregation, long? timeBucket, IReadOnlyCollection<TimeStamp> filterByTs, (long, long)? filterByValue,
            TimeStamp align)
        {
            var args = new List<object>()
                {key, fromTimeStamp.Value, toTimeStamp.Value};
            args.AddFilterByTs(filterByTs);
            args.AddFilterByValue(filterByValue);
            args.AddCount(count);
            args.AddAlign(align);
            args.AddAggregation(aggregation, timeBucket);
            return args;
        }
        

        private static List<object> BuildMultiRangeArgs(TimeStamp fromTimeStamp, TimeStamp toTimeStamp, 
            IReadOnlyCollection<string> filter, long? count, TsAggregation? aggregation, long? timeBucket,
            bool? withLabels, (string, TsReduce)? groupbyTuple, IReadOnlyCollection<TimeStamp> filterByTs,
            (long, long)? filterByValue, IReadOnlyCollection<string> selectLabels)
        {
            var args = new List<object>() {fromTimeStamp.Value, toTimeStamp.Value};
            args.AddFilterByTs(filterByTs);
            args.AddFilterByValue(filterByValue);
            args.AddCount(count);
            args.AddAggregation(aggregation, timeBucket);
            args.AddWithLabels(withLabels, selectLabels);
            args.AddFilters(filter);
            args.AddGroupby(groupbyTuple);
            return args;
        }
    }
}
