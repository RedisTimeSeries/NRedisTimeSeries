using NRedisTimeSeries.Commands;
using NRedisTimeSeries.DataTypes;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace NRedisTimeSeries
{
    /// <summary>
    /// RedisTimeSeries async client API
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
        /// <returns>If the operation executed successfully</returns>
        public static async Task<bool> TimeSeriesCreateAsync(this IDatabase db, string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null)
        {
            var args = new List<object> { key };
            args.AddRetentionTime(retentionTime);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            return ParseBoolean(await db.ExecuteAsync(TS.CREATE, args));
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
        public static async Task<bool> TimeSeriesAlterAsync(this IDatabase db, string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null)
        {
            var args = new List<object> { key };
            args.AddRetentionTime(retentionTime);
            args.AddLabels(labels);
            return ParseBoolean(await db.ExecuteAsync(TS.ALTER, args));
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
        /// <returns>The timestamp value of the new sample</returns>
        public static async Task<TimeStamp> TimeSeriesAddAsync(this IDatabase db, string key, TimeStamp timestamp, double value, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null)
        {
            var args = new List<object> { key, timestamp.Value, value };
            AddRetentionTime(args, retentionTime);
            AddLabels(args, labels);
            AddUncompressed(args, uncompressed);
            return ParseTimeStamp(await db.ExecuteAsync(TS.ADD, args));
        }

        /// <summary>
        /// Append new samples to multiple series.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="sequence">An Collection of (key, timestamp, value) tuples </param>
        /// <returns>List of timestamps of the new samples</returns>
        public static async Task<IReadOnlyList<TimeStamp>> TimeSeriesMAddAsync(this IDatabase db, IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
        {
            var args = new List<object>();
            foreach (var tuple in sequence)
            {
                args.Add(tuple.key);
                args.Add((long)tuple.timestamp);
                args.Add(tuple.value);
            }
            return ParseTimeStampArray(await db.ExecuteAsync(TS.MADD, args));
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
        /// <returns>The latests sample timestamp (updated sample)</returns>
        public static async Task<TimeStamp> TimeSeriesIncrByAsync(this IDatabase db, string key, double value, TimeStamp timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null)
        {
            var args = new List<object> { key, value };
            args.AddTimeStamp(timestamp);
            args.AddRetentionTime(retentionTime);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            return ParseTimeStamp(await db.ExecuteAsync(TS.INCRBY, args));
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
        /// <returns>The latests sample timestamp (updated sample)</returns>
        public static async Task<TimeStamp> TimeSeriesDecrByAsync(this IDatabase db, string key, double value, TimeStamp timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null)
        {
            var args = new List<object> { key, value };
            args.AddTimeStamp(timestamp);
            args.AddRetentionTime(retentionTime);
            args.AddLabels(labels);
            args.AddUncompressed(uncompressed);
            return ParseTimeStamp(await db.ExecuteAsync(TS.DECRBY, args));
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
        public static async Task<bool> TimeSeriesCreateRuleAsync(this IDatabase db, string sourceKey, TimeSeriesRule rule)
        {
            var args = new List<object> { sourceKey };
            args.AddRule(rule);
            return ParseBoolean(await db.ExecuteAsync(TS.CREATERULE, args));
        }

        /// <summary>
        /// Deletes a compaction rule.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="sourceKey">Key name for source time series</param>
        /// <param name="destKey">Key name for destination time series</param>
        /// <returns>If the operation executed successfully</returns>
        public static async Task<bool> TimeSeriesDeleteRuleAsync(this IDatabase db, string sourceKey, string destKey)
        {
            var args = new List<object> { sourceKey, destKey };
            return ParseBoolean(await db.ExecuteAsync(TS.DELETERULE, args));
        }

        #endregion

        #region Query

        /// <summary>
        /// Get the last sample.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="key">Key name for timeseries</param>
        /// <returns>TimeSeriesTuple that represents the last sample. Null if the series is empty. </returns>
        public static async Task<TimeSeriesTuple> TimeSeriesGetAsync(this IDatabase db, string key)
        {
            return ParseTimeSeriesTuple(await db.ExecuteAsync(TS.GET, key));
        }

        /// <summary>
        /// Get the last samples matching the specific filter.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <returns>The command returns the last sample for entries with labels matching the specified filter.</returns>
        public static async Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)>> TimeSeriesMGetAsync(this IDatabase db, IReadOnlyCollection<string> filter, bool? withLabels = null)
        {
            var args = new List<object>();
            args.AddWithLabels(withLabels);
            AddFilters(args, filter);
            return ParseMGetesponse(await db.ExecuteAsync(TS.MGET, args));
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
        /// <returns>A list of TimeSeriesTuple</returns>
        public static async Task<IReadOnlyList<TimeSeriesTuple>> TimeSeriesRangeAsync(this IDatabase db, string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp, long? count = null, Aggregation aggregation = null, long? timeBucket = null)
        {
            var args = new List<object>()
            { key, fromTimeStamp.Value, toTimeStamp.Value };
            args.AddCount(count);
            args.AddAggregation(aggregation, timeBucket);
            return ParseTimeSeriesTupleArray(await db.ExecuteAsync(TS.RANGE, args));
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
        /// <returns>A list of <(key, labels, values)> tuples. Each tuple contains the key name, its labels and the values which satisfies the given range and filters.</returns>
        public static async Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>> TimeSeriesMRangeAsync(this IDatabase db, TimeStamp fromTimeStamp, TimeStamp toTimeStamp, IReadOnlyCollection<string> filter, long? count = null, Aggregation aggregation = null, long? timeBucket = null, bool? withLabels = null)
        {
            var args = new List<object>() { fromTimeStamp.Value, toTimeStamp.Value };
            args.AddCount(count);
            args.AddAggregation(aggregation, timeBucket);
            args.AddWithLabels(withLabels);
            args.AddFilters(filter);

            return ParseMRangeResponse(await db.ExecuteAsync(TS.MRANGE, args));
        }

        #endregion

        #region General

        /// <summary>
        /// Returns the information for a specific time-series key.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="key">Key name for timeseries</param>
        /// <returns>TimeSeriesInformation for the specific key.</returns>
        public static async Task<TimeSeriesInformation> TimeSeriesInfoAsync(this IDatabase db, string key)
        {
            return ParseInfo(await db.ExecuteAsync(TS.INFO, key));
        }

        /// <summary>
        /// Get all the keys matching the filter list.
        /// </summary>
        /// <param name="db">StackExchange.Redis IDatabase instance</param>
        /// <param name="filter">A sequence of filters</param>
        /// <returns>A list of keys with labels matching the filters.</returns>
        public static async Task<IReadOnlyList<string>> TimeSeriesQueryIndexAsync(this IDatabase db, IReadOnlyCollection<string> filter)
        {
            var args = new List<object>(filter);
            return ParseStringArray(await db.ExecuteAsync(TS.QUERYINDEX, args));
        }

        #endregion
    }
}
