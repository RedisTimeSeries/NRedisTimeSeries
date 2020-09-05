using System;
using System.Collections.Generic;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;

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

        private static void AddAggregation(this IList<object> args, Aggregation aggregation, TsTimeBucket timeBucket)
        {
            if (aggregation != null)
            {
                args.Add(CommandArgs.AGGREGATION);
                args.Add(aggregation.Name);
                args.Add(timeBucket.UnixMilliseconds);
            }
        }

        private static void AddFilters(this List<object> args, IReadOnlyCollection<string> filter)
        {
            if (filter == null || filter.Count == 0)
            {
                throw new ArgumentException("There should be at least one filter on MRANGE/MREVRANGE");
            }
            args.Add(CommandArgs.FILTER);
            foreach (string f in filter)
            {
                args.Add(f);
            }
        }

        private static void AddWithLabels(this IList<object> args, bool? withLabels)
        {
            if (withLabels.HasValue && withLabels.Value)
            {
                args.Add(CommandArgs.WITHLABELS);
            }
        }

        private static void AddTimeStamp(this IList<object> args, TsTimeStamp? timeStamp)
        {
            if (timeStamp != null)
            {
                args.Add(CommandArgs.TIMESTAMP);
                args.Add(timeStamp?.UnixMilliseconds);
            }
        }

        private static void AddRule(this IList<object> args, TimeSeriesRule rule)
        {
            args.Add(rule.DestKey);
            args.Add(CommandArgs.AGGREGATION);
            args.Add(rule.Aggregation.Name);
            args.Add(rule.TimeBucket.UnixMilliseconds);
        }

        private static List<object> BuildTsCreateArgs(string key, long? retentionTime, IReadOnlyCollection<TimeSeriesLabel> labels, bool? uncompressed,
            long? chunkSizeBytes)
        {
            var args = new List<object> { key };
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            return args;
        }

        private static List<object> BuildTsAlterArgs(string key, long? retentionTime, IReadOnlyCollection<TimeSeriesLabel> labels)
        {
            var args = new List<object> { key };
            args.AddRetentionTime(retentionTime);
            args.AddLabels(labels);
            return args;
        }

        private static List<object> BuildTsAddArgs(string key, double value, long? retentionTime,
            IReadOnlyCollection<TimeSeriesLabel> labels, bool? uncompressed, long? chunkSizeBytes, TsTimeStamp? timestamp = null)
        {
            var args = new List<object> { key, timestamp?.UnixMilliseconds.ToString() ?? "*", value };
            AddRetentionTime(args, retentionTime);
            AddChunkSize(args, chunkSizeBytes);
            AddLabels(args, labels);
            AddUncompressed(args, uncompressed);
            return args;
        }

        private static List<object> BuildTsIncrDecrByArgs(string key, double value, TsTimeStamp? timestamp, long? retentionTime,
            IReadOnlyCollection<TimeSeriesLabel> labels, bool? uncompressed, long? chunkSizeBytes)
        {
            var args = new List<object> { key, value };
            args.AddTimeStamp(timestamp);
            args.AddRetentionTime(retentionTime);
            args.AddChunkSize(chunkSizeBytes);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            return args;
        }

        private static List<object> BuildTsMaddArgs(IReadOnlyCollection<(string key, TsTimeStamp timestamp, double value)> sequence)
        {
            var args = new List<object>();
            foreach (var (key, timestamp, value) in sequence)
            {
                args.Add(key);
                args.Add(timestamp.UnixMilliseconds);
                args.Add(value);
            }

            return args;
        }

        private static List<object> BuildTsMaddArgs(IReadOnlyCollection<(string key, double value)> sequence)
        {
            var args = new List<object>();
            foreach (var (key, value) in sequence)
            {
                args.Add(key);
                args.Add("*");
                args.Add(value);
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

        private static List<object> BuildRangeArgs(string key, TsTimeStamp fromTimeStamp, TsTimeStamp toTimeStamp, long? count,
            Aggregation aggregation, TsTimeBucket? timeBucket)
        {
            var args = new List<object>() { key, fromTimeStamp.UnixMilliseconds, toTimeStamp.UnixMilliseconds };
            args.AddCount(count);

            if (aggregation != null && !timeBucket.HasValue)
                throw new ArgumentException("RANGE Aggregation should have timeBucket value");

            if (aggregation != null && timeBucket.HasValue)
                args.AddAggregation(aggregation, timeBucket.Value);

            return args;
        }

        private static List<object> BuildMultiRangeArgs(TsTimeStamp fromTimeStamp, TsTimeStamp toTimeStamp, IReadOnlyCollection<string> filter,
            long? count, Aggregation aggregation, TsTimeBucket? timeBucket, bool? withLabels)
        {
            var args = new List<object>() { fromTimeStamp.UnixMilliseconds, toTimeStamp.UnixMilliseconds };
            args.AddCount(count);

            if (aggregation != null && !timeBucket.HasValue)
                throw new ArgumentException("RANGE Aggregation should have timeBucket value");

            if (aggregation != null && timeBucket.HasValue)
                args.AddAggregation(aggregation, timeBucket.Value);

            args.AddWithLabels(withLabels);
            args.AddFilters(filter);

            return args;
        }
    }
}
