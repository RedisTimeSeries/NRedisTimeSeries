using System;
namespace NRedisTimeSeries.Interfaces
{
    public interface ILabel
    {
        string Key { get; }
        string Value { get; }
    }
}
