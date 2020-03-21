using System;
using System.Collections.Generic;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;

namespace NRedisTimeSeries
{
    public static partial class TimeSeriesClient
    {

        #region Create

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="key"></param>
        /// <param name="retentionTime"></param>
        /// <param name="labels"></param>
        /// <param name="uncompressed"></param>
        /// <returns></returns>
        public static bool TimeSeriesCreate(this IDatabase db, string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null)
        {
            var args = new List<object> { key };
            args.AddRetentionTime(retentionTime);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            return ParseBoolean(db.Execute(TS.CREATE, args));
        }

        #endregion


        #region Update

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="key"></param>
        /// <param name="retentionTime"></param>
        /// <param name="labels"></param>
        /// <returns></returns>
        public static bool TimeSeriesAlter(this IDatabase db, string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null)
        {
            var args = new List<object> { key };
            args.AddRetentionTime(retentionTime);
            args.AddLabels(labels);
            return ParseBoolean(db.Execute(TS.ALTER, args));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="key"></param>
        /// <param name="timestamp"></param>
        /// <param name="value"></param>
        /// <param name="retentionTime"></param>
        /// <param name="labels"></param>
        /// <param name="uncompressed"></param>
        /// <returns></returns>
        public static TimeStamp TimeSeriesAdd(this IDatabase db, string key, TimeStamp timestamp, double value, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null)
        {
            var args = new List<object> { key, timestamp.Value, value };
            AddRetentionTime(args, retentionTime);
            AddLabels(args, labels);
            AddUncompressed(args, uncompressed);
            return ParseTimeStamp(db.Execute(TS.ADD, args));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="sequence"></param>
        /// <returns></returns>
        public static IReadOnlyList<TimeStamp> TimeSeriesMAdd(this IDatabase db, IEnumerable<(string key, TimeStamp timestamp, double value)> sequence)
        {
            var args = new List<object>();
            foreach (var tuple in sequence)
            {
                args.Add(tuple.key);
                args.Add((long)tuple.timestamp);
                args.Add(tuple.value);
            }
            return ParseTimeStampArray(db.Execute(TS.MADD, args));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="timestamp"></param>
        /// <param name="retentionTime"></param>
        /// <param name="labels"></param>
        /// <param name="uncompressed"></param>
        /// <returns></returns>
        public static bool TimeSeriesIncerBy(this IDatabase db, string key, double value, TimeStamp timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null)
        {
            var args = new List<object> { key, value };
            args.AddTimeStamp(timestamp);
            args.AddRetentionTime(retentionTime);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            return ParseBoolean(db.Execute(TS.INCRBY, args));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="timestamp"></param>
        /// <param name="retentionTime"></param>
        /// <param name="labels"></param>
        /// <param name="uncompressed"></param>
        /// <returns></returns>
        public static bool TimeSeriesDecrBy(this IDatabase db, string key, double value, TimeStamp timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null)
        {
            var args = new List<object> { key, value };
            args.AddTimeStamp(timestamp);
            args.AddRetentionTime(retentionTime);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            return ParseBoolean(db.Execute(TS.DECRBY, args));
        }

        #endregion


        #region Aggregation, Compaction, Downsampling

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="sourceKey"></param>
        /// <param name="rule"></param>
        /// <returns></returns>
        public static bool TimeSeriesCreateRule(this IDatabase db, string sourceKey, TimeSeriesRule rule)
        {
            var args = new List<object> { sourceKey };
            args.AddRule(rule);
            return ParseBoolean(db.Execute(TS.CREATERULE, args));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="sourceKey"></param>
        /// <param name="destKey"></param>
        /// <returns></returns>
        public static bool TimeSeriesDeleteRule(this IDatabase db, string sourceKey, string destKey)
        {
            var args = new List<object> { sourceKey, destKey };
            return ParseBoolean(db.Execute(TS.DELETERULE, args));
        }

        #endregion


        #region Query

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static TimeSeriesTuple TimeSeriesGet(this IDatabase db, string key)
        {
            return ParseTimeSeriesTuple(db.Execute(TS.GET, key));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="filter"></param>
        /// <param name="withLabels"></param>
        /// <returns></returns>
        public static IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> TimeSeriesMGet(this IDatabase db, IReadOnlyCollection<string> filter, bool? withLabels)
        {
            var args = new List<object>();
            AddFilters(args, filter);
            args.AddWithLabels(withLabels);
            return ParseMResponse(db.Execute(TS.MGET, args));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="key"></param>
        /// <param name="fromTimeStamp"></param>
        /// <param name="toTimeStamp"></param>
        /// <param name="count"></param>
        /// <param name="aggregation"></param>
        /// <param name="timeBucket"></param>
        /// <returns></returns>
        public static IReadOnlyList<TimeSeriesTuple> TimeSeriesRange(this IDatabase db, string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp, long? count = null, Aggregation aggregation = null, long? timeBucket = null)
        {
            var args = new List<object>()
            { key, fromTimeStamp.Value, toTimeStamp.Value };
            args.AddCount(count);
            args.AddAggregation(aggregation, timeBucket);
            return ParseTimeSeriesTupleArray(db.Execute(TS.RANGE, args));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="fromTimeStamp"></param>
        /// <param name="toTimeStamp"></param>
        /// <param name="filter"></param>
        /// <param name="count"></param>
        /// <param name="aggregation"></param>
        /// <param name="timeBucket"></param>
        /// <param name="withLabels"></param>
        /// <returns></returns>
        public static IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> TimeSeriesMRange(this IDatabase db, TimeStamp fromTimeStamp, TimeStamp toTimeStamp, IReadOnlyCollection<string> filter, long? count = null, Aggregation aggregation = null, long? timeBucket = null, bool? withLabels = null)
        {
            var args = new List<object>() { fromTimeStamp.Value, toTimeStamp.Value };
            args.AddFilters(filter);
            args.AddCount(count);
            args.AddAggregation(aggregation, timeBucket);
            args.AddWithLabels(withLabels);

            return ParseMResponse(db.Execute(TS.MRANGE, args));
        }

        #endregion


        #region General

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static TimeSeriesInformation TimeSeriesInfo(this IDatabase db, string key)
        {
            return ParseInfo(db.Execute(TS.INFO, key));
        }


        public static IReadOnlyList<string> TimeSeriesQueryIndex(this IDatabase db, IReadOnlyCollection<string> filter)
        {
            var args = new List<object>(filter);
            return ParseStringArray(db.Execute(TS.QUERYINDEX, args));
        }


        #endregion

    }
}
