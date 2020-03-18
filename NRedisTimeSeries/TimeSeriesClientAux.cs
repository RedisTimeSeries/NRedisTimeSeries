using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;

namespace NRedisTimeSeries
{
    public static partial class TimeSeriesClient
    {
        private static void AddRetentionTime(IList<object> args, long? retentionTime)
        {
            if (retentionTime.HasValue)
            {
                args.Add(CommandArgs.RETENTION);
                args.Add(retentionTime);
            }
        }

        private static void AddLabels(IList<object> args, IReadOnlyCollection<Label> labels)
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

        private static void AddUncompressed(IList<object> args, bool? uncompressed)
        {
            if (uncompressed.HasValue)
            {
                args.Add(CommandArgs.UNCOMPRESSED);
            }
        }
    }
}
