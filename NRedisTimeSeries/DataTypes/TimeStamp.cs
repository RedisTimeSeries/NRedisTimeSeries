using System;
using System.Collections.Generic;

namespace NRedisTimeSeries
{
    public class TimeStamp : IEquatable<TimeStamp>
    {
        private static readonly string[] constants = { "-", "+", "*" };

        private readonly object value;

        public TimeStamp(long timestamp) => value = timestamp;

        public TimeStamp(DateTime dateTime) => value = dateTime.Ticks;

        public TimeStamp(string timestamp)
        {
            if (Array.IndexOf(constants, timestamp) == -1)
            {
                throw new NotSupportedException(string.Format("The string {0} cannot be used", timestamp));
            }
            value = timestamp;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as TimeStamp);
        }

        public bool Equals(TimeStamp other)
        {
            return other != null &&
                   EqualityComparer<object>.Default.Equals(value, other.value);
        }

        public override int GetHashCode()
        {
            return -1584136870 + EqualityComparer<object>.Default.GetHashCode(value);
        }

        public static implicit operator TimeStamp(long l) => new TimeStamp(l);
        public static implicit operator long(TimeStamp ts) => ts.value is long ? (long)ts.value :
            throw new InvalidCastException("Cannot convert string timestamp to long");
        public static implicit operator TimeStamp(string s) => new TimeStamp(s);
        public static implicit operator string(TimeStamp ts) => ts.value is string ? (string)ts.value :
            throw new InvalidCastException("Cannot convert long timestamp to string");
        public static implicit operator TimeStamp(DateTime dateTime) => new TimeStamp(dateTime);
        public static implicit operator DateTime(TimeStamp timeStamp) => new DateTime(timeStamp);



    }
}
