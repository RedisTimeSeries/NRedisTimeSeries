using System;
using System.Collections.Generic;

namespace NRedisTimeSeries.DataTypes
{
    public class TimeSeriesLabel
    {
        public string Key { get; }
        public string Value { get; }

        public TimeSeriesLabel(string key, string value) => (Key ,Value) = (key, value);

        public override bool Equals(object obj)
        {
            return obj is TimeSeriesLabel label &&
                   Key == label.Key &&
                   Value == label.Value;
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
