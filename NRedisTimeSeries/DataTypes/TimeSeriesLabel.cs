using System;
using System.Collections.Generic;

namespace NRedisTimeSeries.DataTypes
{
    public class TimeSeriesLabel : IEquatable<TimeSeriesLabel>
    {
        public string Key { get; }
        public string Value { get; }

        public TimeSeriesLabel(string key, string value) => (Key ,Value) = (key, value);

        public override bool Equals(object obj)
        {
            return Equals(obj as TimeSeriesLabel);
        }

        public bool Equals(TimeSeriesLabel other)
        {
            return other != null &&
                   Key == other.Key &&
                   Value == other.Value;
        }

        public override int GetHashCode()
        {
            var hashCode = 206514262;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(Key);
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(Value);
            return hashCode;
        }
    }
}
