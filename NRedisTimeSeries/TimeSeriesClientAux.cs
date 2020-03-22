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

        private static void AddAggregation(this IList<object> args, Aggregation aggregation, long? timeBucket)
        {
            if(aggregation != null)
            {
                args.Add(CommandArgs.AGGREGATION);
                args.Add(aggregation.Name);
                if (!timeBucket.HasValue)
                {
                    throw new ArgumentException("RAGNE Aggregation should have timeBucket value");
                }
                args.Add(timeBucket.Value);
            }
        }

        private static void AddFilters(this List<object> args, IReadOnlyCollection<string> filter)
        {
            if(filter == null || filter.Count == 0)
            {
                throw new ArgumentException("There should be at least one filter on MRANGE");
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
            args.Add(rule.Aggregation);
            args.Add(rule.BucketTime);
        }
    }
}
