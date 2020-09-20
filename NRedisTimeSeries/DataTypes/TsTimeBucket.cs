using System;

namespace NRedisTimeSeries.DataTypes
{
    public readonly struct TsTimeBucket : IEquatable<TsTimeBucket>
    {
        private const long _minUnixMilliseconds = 1;                        // 01/01/1970 12:00:00 AM (+1ms)
        private const long _maxUnixMilliseconds = 253_402_271_999_998;      // 12/31/9999 03:59:59 PM

        public long UnixMilliseconds { get; }

        public static readonly TsTimeBucket MinValue = new TsTimeBucket(_minUnixMilliseconds);
        public static readonly TsTimeBucket MaxValue = new TsTimeBucket(_maxUnixMilliseconds);

        public TsTimeBucket(long unixMilliseconds)
        {
            if (unixMilliseconds < _minUnixMilliseconds)
                throw new ArgumentOutOfRangeException(nameof(unixMilliseconds), $"Must be {_minUnixMilliseconds}ms or greater");

            if (unixMilliseconds > _maxUnixMilliseconds)
                throw new ArgumentOutOfRangeException(nameof(unixMilliseconds), $"Must be {_maxUnixMilliseconds}ms or less");

            UnixMilliseconds = unixMilliseconds;
        }

        public TsTimeBucket(TsTimeStamp fromTimeStamp, TsTimeStamp toTimeStamp)
        {
            var unixMilliseconds = toTimeStamp.UnixMilliseconds - fromTimeStamp.UnixMilliseconds;

            if (unixMilliseconds < 1)
                throw new ArgumentException($"The time bucket must be {_minUnixMilliseconds}ms or greater");

            UnixMilliseconds = unixMilliseconds;
        }

        public static bool operator ==(TsTimeBucket left, TsTimeBucket right) => Equals(left, right);

        public static bool operator !=(TsTimeBucket left, TsTimeBucket right) => !Equals(left, right);

        public override bool Equals(object obj) => obj is TsTimeBucket other && Equals(other);

        public bool Equals(TsTimeBucket other) => UnixMilliseconds == other.UnixMilliseconds;

        public override int GetHashCode() => UnixMilliseconds.GetHashCode();

        public override string ToString() => UnixMilliseconds.ToString();
    }
}
