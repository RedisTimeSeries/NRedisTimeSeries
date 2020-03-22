using System;
using System.Collections.Generic;

namespace NRedisTimeSeries.DataTypes
{
    public class TimeSeriesRule
    {
        public string DestKey { get; private set; }
        public long TimeBucket { get; private set; }
        public Aggregation Aggregation { get; private set; }

        public TimeSeriesRule(string destKey, long timeBucket, Aggregation aggregation)
        {
            DestKey = destKey;
            TimeBucket = timeBucket;
            Aggregation = aggregation;
        }

        public override bool Equals(object obj)
        {
            return obj is TimeSeriesRule rule &&
                   DestKey == rule.DestKey &&
                   TimeBucket == rule.TimeBucket &&
                   EqualityComparer<Aggregation>.Default.Equals(Aggregation, rule.Aggregation);
        }

        public override int GetHashCode()
        {
            var hashCode = 1554951643;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(DestKey);
            hashCode = hashCode * -1521134295 + TimeBucket.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<Aggregation>.Default.GetHashCode(Aggregation);
            return hashCode;
        }
    }
}
