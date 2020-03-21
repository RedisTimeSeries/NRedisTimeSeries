using System;
using System.Collections.Generic;

namespace NRedisTimeSeries.DataTypes
{
    public class TimeSeriesRule : IEquatable<TimeSeriesRule>
    {
        public string DestKey { get; private set; }
        public long BucketTime { get; private set; }
        public Aggregation Aggregation { get; private set; }

        public TimeSeriesRule(string destKey, long bucketTime, Aggregation aggregation)
        {
            DestKey = destKey;
            BucketTime = bucketTime;
            Aggregation = aggregation;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as TimeSeriesRule);
        }

        public bool Equals(TimeSeriesRule other)
        {
            return other != null &&
                   DestKey == other.DestKey &&
                   BucketTime == other.BucketTime &&
                   EqualityComparer<Aggregation>.Default.Equals(Aggregation, other.Aggregation);
        }

        public override int GetHashCode()
        {
            var hashCode = -414437737;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(DestKey);
            hashCode = hashCode * -1521134295 + BucketTime.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<Aggregation>.Default.GetHashCode(Aggregation);
            return hashCode;
        }
    }
}
