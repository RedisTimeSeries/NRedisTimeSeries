using System;
using System.Collections.Generic;

namespace NRedisTimeSeries.DataTypes
{

    public class TimeSeriesInformation
    {
        public long TotalSamples { get; private set; }
        public long MemoryUsage { get; private set; }
        public TimeStamp FirstTimeStamp { get; private set; }
        public TimeStamp LastTimeStap { get; private set; }
        public long RetentionTime { get; private set; }
        public long ChunkCount { get; private set; }
        public long MaxSamplesPerChunk { get; private set; }
        public IReadOnlyList<TimeSeriesLabel> Labels { get; private set; }
        public string SourceKey { get; private set; }
        public IReadOnlyList<TimeSeriesRule> Rules { get; private set; }

        public TimeSeriesInformation(long totalSamples, long memoryUsage, TimeStamp firstTimeStamp, TimeStamp lastTimeStap, long retentionTime, long chunkCount, long maxSamplesPerChunk, IReadOnlyList<TimeSeriesLabel> labels, string sourceKey, IReadOnlyList<TimeSeriesRule> rules)
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
