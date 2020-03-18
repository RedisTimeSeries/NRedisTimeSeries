using System;
namespace NRedisTimeSeries
{
    /// <summary>
    /// 
    /// </summary>
    public interface IValue
    {
        /// <summary>
        /// 
        /// </summary>
        TimeStamp Time { get; }

        /// <summary>
        /// 
        /// </summary>
        double Val { get; }
    }
}
