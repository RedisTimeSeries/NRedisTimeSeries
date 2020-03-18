using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries
{
    public static partial class TimeSeriesClient
    {
        public static bool TimeSeriesCreate(this IDatabase db, string key, long? retentionTime = null, IReadOnlyCollection<Label> labels = null, bool? uncompressed = null)
        {
            var args = new List<object>
            {
                key
            };

            AddRetentionTime(args, retentionTime);
            AddLabels(args, labels);
            AddUncompressed(args, uncompressed);

            return ParseBoolean(db.Execute(TS.CREATE, args));
        }

        public static Value TimeSeriesGet(this IDatabase db, string key)
        {
            return ParseValue(db.Execute(TS.GET, key));
        }

        public static TimeStamp TimeSeriesAdd(this IDatabase db, string key, TimeStamp timestamp, double value, long? retentionTime = null, IReadOnlyCollection<Label> labels = null, bool? uncompressed = null)
        {
            var args = new List<object>
            {
                key,
                (long)timestamp,
                value
            };

            AddRetentionTime(args, retentionTime);
            AddLabels(args, labels);
            AddUncompressed(args, uncompressed);

            return ParseTimeStamp(db.Execute(TS.ADD, args));
        }

        //public bool TimeSeriesAlter(string key, long? retentionTime = null, IReadOnlyCollection<ILabel> labels = null)
        //{
        //    throw new NotImplementedException();
        //}

        //public bool TimeSeriesCreateRule(string sourceKey, string destKey, Aggregation aggregation, long timeBucket)
        //{
        //    throw new NotImplementedException();
        //}

        //public bool TimeSeriesDecrBy(string key, double value, TimeStamp timestamp = null, long? retentionTime = null, IReadOnlyCollection<ILabel> labels = null, bool? uncompressed = null)
        //{
        //    throw new NotImplementedException();
        //}

        //public bool TimeSeriesDeleteRule(string sourceKey, string destKey)
        //{
        //    throw new NotImplementedException();
        //}


        //public bool TimeSeriesIncerBy(string key, double value, TimeStamp timestamp = null, long? retentionTime = null, IReadOnlyCollection<ILabel> labels = null, bool? uncompressed = null)
        //{
        //    throw new NotImplementedException();
        //}

        //public IReadOnlyList<TimeStamp> TimeSeriesMAdd(IEnumerable<(string key, double value, long? timestamp)> sequence)
        //{
        //    throw new NotImplementedException();
        //}

        //public IReadOnlyList<(string key, IReadOnlyList<ILabel> labels, IReadOnlyList<IValue> values)> TimeSeriesMGet(IReadOnlyCollection<string> filter, bool? withLabels)
        //{
        //    throw new NotImplementedException();
        //}

        //public IReadOnlyList<(string key, IReadOnlyList<ILabel> labels, IReadOnlyList<IValue> values)> TimeSeriesMRange(TimeStamp fromTimeStamp, TimeStamp toTimeStamp, IReadOnlyCollection<string> filter, long? count = null, Aggregation aggregation = null, long? timeBucket = null, bool? withLabels = null)
        //{
        //    throw new NotImplementedException();
        //}

        //public IReadOnlyList<IValue> TimeSeriesRange(TimeStamp fromTimeStamp, TimeStamp toTimeStamp, long? count = null, Aggregation aggregation = null, long? timeBucket = null)
        //{
        //    throw new NotImplementedException();
        //}
    }
}
