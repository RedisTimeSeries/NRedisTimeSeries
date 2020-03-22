using System;
using System.Collections.Generic;

namespace NRedisTimeSeries.DataTypes
{
    public class TimeSeriesRule
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
            return obj is TimeSeriesRule rule &&
                   BucketTime == rule.BucketTime &&
                   EqualityComparer<Aggregation>.Default.Equals(Aggregation, rule.Aggregation);
        }

        public override int GetHashCode()
        {
            var hashCode = -160123813;
            hashCode = hashCode * -1521134295 + BucketTime.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<Aggregation>.Default.GetHashCode(Aggregation);
            return hashCode;
        }
    }
}
