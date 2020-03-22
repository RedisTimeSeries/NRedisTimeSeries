using System;
using System.Collections.Generic;

namespace NRedisTimeSeries
{
    /// <summary>
    /// 
    /// </summary>
    public class TimeSeriesTuple
    {
        public TimeStamp Time { get; }

        public double Val { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="val"></param>
        public TimeSeriesTuple(TimeStamp time, double val) => (Time, Val) = (time, val);

        public override bool Equals(object obj)
        {
            return obj is TimeSeriesTuple tuple &&
                   EqualityComparer<TimeStamp>.Default.Equals(Time, tuple.Time) &&
                   Val == tuple.Val;
        }

        public override int GetHashCode()
        {
            var hashCode = 459537088;
            hashCode = hashCode * -1521134295 + EqualityComparer<TimeStamp>.Default.GetHashCode(Time);
            hashCode = hashCode * -1521134295 + Val.GetHashCode();
            return hashCode;
        }
    }
}
