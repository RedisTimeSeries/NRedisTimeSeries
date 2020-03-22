using System;
using System.Collections.Generic;

namespace NRedisTimeSeries
{
    public class TimeStamp
    {
        private static readonly string[] constants = { "-", "+", "*" };

        public object Value { get; private set; }

        public TimeStamp(long timestamp) => Value = timestamp;

        public TimeStamp(DateTime dateTime) => Value = dateTime.Ticks;

        public TimeStamp(string timestamp)
        {
            if (Array.IndexOf(constants, timestamp) == -1)
            {
                throw new NotSupportedException(string.Format("The string {0} cannot be used", timestamp));
            }
            Value = timestamp;
        }

        public static implicit operator TimeStamp(long l) => new TimeStamp(l);
        public static implicit operator long(TimeStamp ts) => ts.Value is long ? (long)ts.Value :
            throw new InvalidCastException("Cannot convert string timestamp to long");
        public static implicit operator TimeStamp(string s) => new TimeStamp(s);
        public static implicit operator string(TimeStamp ts) => ts.Value.ToString();
        public static implicit operator TimeStamp(DateTime dateTime) => new TimeStamp(dateTime);
        public static implicit operator DateTime(TimeStamp timeStamp) => new DateTime(timeStamp);

        public override bool Equals(object obj)
        {
            return obj is TimeStamp stamp &&
                   EqualityComparer<object>.Default.Equals(Value, stamp.Value);
        }

        public override int GetHashCode()
        {
            return -1937169414 + EqualityComparer<object>.Default.GetHashCode(Value);
        }
    }
}
