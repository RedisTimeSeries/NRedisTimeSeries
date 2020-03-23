using System;
using System.Collections.Generic;

namespace NRedisTimeSeries.DataTypes
{

    /// <summary>
    /// This class represents the response for TS.INFO command.
    /// This object has Read-only properties and cannot be generated outside a TS.INFO response.
    /// </summary>
    public class TimeSeriesInformation
    {

        /// <summary>
        /// Total samples in the time-series.
        /// </summary>
        public long TotalSamples { get; private set; }

        /// <summary>
        /// Total number of bytes allocated for the time-series.
        /// </summary>
        public long MemoryUsage { get; private set; }

        /// <summary>
        /// First timestamp present in the time-series.
        /// </summary>
        public TimeStamp FirstTimeStamp { get; private set; }

        /// <summary>
        /// Last timestamp present in the time-series.
        /// </summary>
        public TimeStamp LastTimeStap { get; private set; }

        /// <summary>
        /// Retention time, in milliseconds, for the time-series.
        /// </summary>
        public long RetentionTime { get; private set; }

        /// <summary>
        /// Number of Memory Chunks used for the time-series.
        /// </summary>
        public long ChunkCount { get; private set; }

        /// <summary>
        /// Maximum Number of samples per Memory Chunk.
        /// </summary>
        public long MaxSamplesPerChunk { get; private set; }

        /// <summary>
        /// A readonly list of TimeSeriesLabel that represent metadata labels of the time-series.
        /// </summary>
        public IReadOnlyList<TimeSeriesLabel> Labels { get; private set; }

        /// <summary>
        /// Source key for the queries time series key.
        /// </summary>
        public string SourceKey { get; private set; }

        /// <summary>
        /// A readonly list of TimeSeriesRules that represent compaction Rules of the time-series.
        /// </summary>
        public IReadOnlyList<TimeSeriesRule> Rules { get; private set; }

        internal TimeSeriesInformation(long totalSamples, long memoryUsage, TimeStamp firstTimeStamp, TimeStamp lastTimeStap, long retentionTime, long chunkCount, long maxSamplesPerChunk, IReadOnlyList<TimeSeriesLabel> labels, string sourceKey, IReadOnlyList<TimeSeriesRule> rules)
        {
            TotalSamples = totalSamples;
            MemoryUsage = memoryUsage;
            FirstTimeStamp = firstTimeStamp;
            LastTimeStap = lastTimeStap;
            RetentionTime = retentionTime;
            ChunkCount = chunkCount;
            MaxSamplesPerChunk = maxSamplesPerChunk;
            Labels = labels;
            SourceKey = sourceKey;
            Rules = rules;
        }
    }
}
