using System;
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
    }
}
