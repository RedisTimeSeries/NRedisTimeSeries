using System;
using System.Collections.Generic;
using StackExchange.Redis;
using NRedisTimeSeries.Commands;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;

namespace NRedisTimeSeries
{
    /// <summary>
    /// RedisTimeSeries client API
    /// </summary>
    public static partial class TimeSeriesClient
    {
        #region Create

        /// <summary>
        /// Create a new time-series.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <param name="duplicatePolicy">Optinal: Define handling of duplicate samples behavior (avalible for RedisTimeseries >= 1.4)</param>
        /// <returns>If the operation executed successfully</returns>
        public static bool TimeSeriesCreate(this IDatabase db, string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var args = BuildTsCreateArgs(key, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return ParseBoolean(db.Execute(TS.CREATE, args));
        }

        #endregion

        #region Update

        /// <summary>
        /// Update the retention, labels of an existing key.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <returns>If the operation executed successfully</returns>
        public static bool TimeSeriesAlter(this IDatabase db, string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null)
        {
            var args = BuildTsAlterArgs(key, retentionTime, labels);
            return ParseBoolean(db.Execute(TS.ALTER, args));
        }

        /// <summary>
        /// Append (or create and append) a new sample to the series.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="timestamp">TimeStamp to add. UNIX timestamp of the sample. * can be used for automatic timestamp (using the system clock)</param>
        /// <param name="value">Numeric data value of the sample.</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <param name="duplicatePolicy">Optioal: overwrite key and database configuration for DUPLICATE_POLICY</param>
        /// <returns>The timestamp value of the new sample</returns>
        public static TimeStamp TimeSeriesAdd(this IDatabase db, string key, TimeStamp timestamp, double value, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var args = BuildTsAddArgs(key, timestamp, value, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return ParseTimeStamp(db.Execute(TS.ADD, args));
        }

        /// <summary>
        /// Append new samples to multiple series.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="sequence">An Collection of (key, timestamp, value) tuples </param>
        /// <returns>List of timestamps of the new samples</returns>
        public static IReadOnlyList<TimeStamp> TimeSeriesMAdd(this IDatabase db, IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
        {
            var args = BuildTsMaddArgs(sequence);
            return ParseTimeStampArray(db.Execute(TS.MADD, args));
        }

        /// <summary>
        /// Creates a new sample that increments the latest sample's value.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="value">Delta to add</param>
        /// <param name="timestamp">Optional: TimeStamp to add. UNIX timestamp of the sample. * can be used for automatic timestamp (using the system clock)</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <returns>The latests sample timestamp (updated sample)</returns>
        public static TimeStamp TimeSeriesIncrBy(this IDatabase db, string key, double value, TimeStamp timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var args = BuildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return ParseTimeStamp(db.Execute(TS.INCRBY, args));
        }

        /// <summary>
        /// Creates a new sample that decrements the latest sample's value.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="value">Delta to substract</param>
        /// <param name="timestamp">Optional: TimeStamp to add. UNIX timestamp of the sample. * can be used for automatic timestamp (using the system clock)</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TSDB chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <returns>The latests sample timestamp (updated sample)</returns>
        public static TimeStamp TimeSeriesDecrBy(this IDatabase db, string key, double value, TimeStamp timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var args = BuildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return ParseTimeStamp(db.Execute(TS.DECRBY, args));
        }

        /// <summary>
        /// Delete data points for a given timeseries and interval range in the form of start and end delete timestamps.
        /// The given timestamp interval is closed (inclusive), meaning start and end data points will also be deleted.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range deletion.</param>
        /// <param name="toTimeStamp">End timestamp for the range deletion.</param>
        /// <returns>The count of deleted items</returns>
        public static long TimeSeriesDel(this IDatabase db, string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
        {
            var args = BuildTsDelArgs(key, fromTimeStamp, toTimeStamp);
            return ParseLong(db.Execute(TS.DEL, args));
        }

        #endregion

        #region Aggregation, Compaction, Downsampling

        /// <summary>
        /// Create a compaction rule.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="sourceKey">Key name for source time series</param>
        /// <param name="rule">TimeSeries rule:
        /// Key name for destination time series, Aggregation type and Time bucket for aggregation in milliseconds</param>
        /// <returns>If the operation executed successfully</returns>
        public static bool TimeSeriesCreateRule(this IDatabase db, string sourceKey, TimeSeriesRule rule)
        {
            var args = new List<object> { sourceKey };
            args.AddRule(rule);
            return ParseBoolean(db.Execute(TS.CREATERULE, args));
        }

        /// <summary>
        /// Deletes a compaction rule.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="sourceKey">Key name for source time series</param>
        /// <param name="destKey">Key name for destination time series</param>
        /// <returns>If the operation executed successfully</returns>
        public static bool TimeSeriesDeleteRule(this IDatabase db, string sourceKey, string destKey)
        {
            var args = new List<object> { sourceKey, destKey };
            return ParseBoolean(db.Execute(TS.DELETERULE, args));
        }

        #endregion

        #region Query

        /// <summary>
        /// Get the last sample.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="key">Key name for timeseries</param>
        /// <returns>TimeSeriesTuple that represents the last sample. Null if the series is empty. </returns>
        public static TimeSeriesTuple TimeSeriesGet(this IDatabase db, string key)
        {
            return ParseTimeSeriesTuple(db.Execute(TS.GET, key));
        }

        /// <summary>
        /// Get the last samples matching the specific filter.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <returns>The command returns the last sample for entries with labels matching the specified filter.</returns>
        public static IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> TimeSeriesMGet(this IDatabase db, IReadOnlyCollection<string> filter, bool? withLabels = null)
        {
            var args = BuildTsMgetArgs(filter, withLabels);
            return ParseMGetesponse(db.Execute(TS.MGET, args));
        }

        /// <summary>
        /// Query a range.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range query. "-" can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="count">Optional: Returned list size.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of TimeSeriesTuple</returns>
        public static IReadOnlyList<TimeSeriesTuple> TimeSeriesRange(this IDatabase db, 
            string key, 
            TimeStamp fromTimeStamp, 
            TimeStamp toTimeStamp, 
            long? count = null, 
            TsAggregation? aggregation = null, 
            long? timeBucket = null,
            IReadOnlyCollection<TimeStamp> filterByTs = null,
            (long, long)? filterByValue = null,
            TimeStamp align = null)
        {
            var args = BuildRangeArgs(key, fromTimeStamp, toTimeStamp, count, aggregation, timeBucket, filterByTs, filterByValue, align);
            return ParseTimeSeriesTupleArray(db.Execute(TS.RANGE, args));
        }

        /// <summary>
        /// Query a range in reverse order.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range query. "-" can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="count">Optional: Returned list size.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of TimeSeriesTuple</returns>
        public static IReadOnlyList<TimeSeriesTuple> TimeSeriesRevRange(this IDatabase db, 
            string key, 
            TimeStamp fromTimeStamp, 
            TimeStamp toTimeStamp, 
            long? count = null, 
            TsAggregation? aggregation = null, 
            long? timeBucket = null,
            IReadOnlyCollection<TimeStamp> filterByTs = null,
            (long, long)? filterByValue = null,
            TimeStamp align = null)
        {
            var args = BuildRangeArgs(key, fromTimeStamp, toTimeStamp, count, aggregation, timeBucket, filterByTs, filterByValue, align);
            return ParseTimeSeriesTupleArray(db.Execute(TS.REVRANGE, args));
        }

        /// <summary>
        /// Query a timestamp range across multiple time-series by filters.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="fromTimeStamp"> Start timestamp for the range query. - can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="count">Optional: Maximum number of returned results per time-series.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <param name="groupbyTuple">Optional: Grouping by fields the results, and applying reducer functions on each group.</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="selectLabels">Optional: Include in the reply only a subset of the key-value pair labels of a series.</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of (key, labels, values) tuples. Each tuple contains the key name, its labels and the values which satisfies the given range and filters.</returns>
        public static IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> TimeSeriesMRange(this IDatabase db, 
            TimeStamp fromTimeStamp, 
            TimeStamp toTimeStamp, 
            IReadOnlyCollection<string> filter, 
            long? count = null, 
            TsAggregation? aggregation = null, 
            long? timeBucket = null, 
            bool? withLabels = null,
            (string, TsReduce)? groupbyTuple = null,
            IReadOnlyCollection<TimeStamp> filterByTs = null,
            (long, long)? filterByValue = null,
            IReadOnlyCollection<string> selectLabels = null,
            TimeStamp align = null)
        {
            var args = BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, count, aggregation, timeBucket, withLabels, groupbyTuple, filterByTs, filterByValue, selectLabels, align);
            return ParseMRangeResponse(db.Execute(TS.MRANGE, args));
        }

        /// <summary>
        /// Query a timestamp range in reverse order across multiple time-series by filters.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="fromTimeStamp"> Start timestamp for the range query. - can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="count">Optional: Maximum number of returned results per time-series.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <param name="groupbyTuple">Optional: Grouping by fields the results, and applying reducer functions on each group.</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="selectLabels">Optional: Include in the reply only a subset of the key-value pair labels of a series.</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of (key, labels, values) tuples. Each tuple contains the key name, its labels and the values which satisfies the given range and filters.</returns>
        public static IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> TimeSeriesMRevRange(this IDatabase db, 
            TimeStamp fromTimeStamp, 
            TimeStamp toTimeStamp, 
            IReadOnlyCollection<string> filter, 
            long? count = null, 
            TsAggregation? aggregation = null, 
            long? timeBucket = null, 
            bool? withLabels = null, 
            (string, TsReduce)? groupbyTuple = null,
            IReadOnlyCollection<TimeStamp> filterByTs = null,
            (long, long)? filterByValue = null,
            IReadOnlyCollection<string> selectLabels = null,
            TimeStamp align = null)
        {
            var args = BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, count, aggregation, timeBucket, withLabels, groupbyTuple, filterByTs, filterByValue, selectLabels, align);
            return ParseMRangeResponse(db.Execute(TS.MREVRANGE, args));
        }

        #endregion

        #region General

        /// <summary>
        /// Returns the information for a specific time-series key.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="key">Key name for timeseries</param>
        /// <returns>TimeSeriesInformation for the specific key.</returns>
        public static TimeSeriesInformation TimeSeriesInfo(this IDatabase db, string key)
        {
            return ParseInfo(db.Execute(TS.INFO, key));
        }

        /// <summary>
        /// Get all the keys matching the filter list.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="filter">A sequence of filters</param>
        /// <returns>A list of keys with labels matching the filters.</returns>
        public static IReadOnlyList<string> TimeSeriesQueryIndex(this IDatabase db, IReadOnlyCollection<string> filter)
        {
            var args = new List<object>(filter);
            return ParseStringArray(db.Execute(TS.QUERYINDEX, args));
        }

        #endregion
    }
}
