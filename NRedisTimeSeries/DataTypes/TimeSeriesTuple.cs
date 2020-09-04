using System.Collections.Generic;

namespace NRedisTimeSeries.DataTypes
{
    /// <summary>
    /// A class represents time-series timestamp-value pair
    /// </summary>
    public class TimeSeriesTuple
    {
        /// <summary>
        /// Tuple key - timestamp.
        /// </summary>
        public TsTimeStamp Time { get; }

        /// <summary>
        /// Tuple value
        /// </summary>
        public double Val { get; }

        /// <summary>
        /// Create new TimeSeriesTuple.
        /// </summary>
        /// <param name="time">Timestamp</param>
        /// <param name="val">Value</param>
        public TimeSeriesTuple(TsTimeStamp time, double val) => (Time, Val) = (time, val);

        /// <summary>
        /// Equality of TimeSeriesTuple objects
        /// </summary>
        /// <param name="obj">Object to compare</param>
        /// <returns>If two TimeSeriesTuple objects are equal</returns>
        public override bool Equals(object obj) => 
            obj is TimeSeriesTuple tuple && 
            EqualityComparer<TsTimeStamp>.Default.Equals(Time, tuple.Time) && 
            Val == tuple.Val;

        /// <summary>
        /// TimeSeriesTuple object hash code.
        /// </summary>
        /// <returns>TimeSeriesTuple object hash code.</returns>
        public override int GetHashCode()
        {
            var hashCode = 459537088;
            hashCode = (hashCode * -1521134295) + EqualityComparer<TsTimeStamp>.Default.GetHashCode(Time);
            hashCode = (hashCode * -1521134295) + Val.GetHashCode();
            return hashCode;
        }
    }
}
