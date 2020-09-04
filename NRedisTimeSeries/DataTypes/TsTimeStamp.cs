using System;

namespace NRedisTimeSeries.DataTypes
{
    public readonly struct TsTimeStamp : IEquatable<TsTimeStamp>
    {
        private const long _minUnixMilliseconds = 0;                        // 01/01/1970 12:00:00 AM
        private const long _maxUnixMilliseconds = 253_402_271_999_999;      // 12/31/9999 03:59:59 PM
        private const long _epochTicks = 621_355_968_000_000_000;           // 01/01/1970 12:00:00 AM

        public long UnixMilliseconds { get; }

        public DateTime UtcDateTime => DateTimeOffset.FromUnixTimeMilliseconds(UnixMilliseconds).UtcDateTime;

        public static readonly TsTimeStamp MinValue = new TsTimeStamp(_minUnixMilliseconds);
        public static readonly TsTimeStamp MaxValue = new TsTimeStamp(_maxUnixMilliseconds);

        public TsTimeStamp(long unixMilliseconds)
        {
            if (unixMilliseconds < _minUnixMilliseconds)
                throw new ArgumentOutOfRangeException(nameof(unixMilliseconds), $"Must be {_minUnixMilliseconds}ms or greater");

            if (unixMilliseconds > _maxUnixMilliseconds)
                throw new ArgumentOutOfRangeException(nameof(unixMilliseconds), $"Must be {_maxUnixMilliseconds}ms or less");

            UnixMilliseconds = unixMilliseconds;
        }

        public TsTimeStamp(DateTime dateTime)
        {
            if (dateTime.Kind != DateTimeKind.Utc)
                throw new ArgumentException("DateTime Kind must be UTC", nameof(dateTime));

            if (dateTime.Ticks < _epochTicks)
                throw new ArgumentOutOfRangeException(nameof(dateTime), $"Must be Unix Epoch time or greater");

            UnixMilliseconds = new DateTimeOffset(dateTime).ToUnixTimeMilliseconds();
        }

        public static implicit operator DateTime(TsTimeStamp timeStamp) => timeStamp.UtcDateTime;

        public static implicit operator TsTimeStamp(DateTime dateTime) => new TsTimeStamp(dateTime);

        public static bool operator ==(TsTimeStamp left, TsTimeStamp right) => Equals(left, right);

        public static bool operator !=(TsTimeStamp left, TsTimeStamp right) => !Equals(left, right);

        public override bool Equals(object obj) => obj is TsTimeStamp other && Equals(other);

        public bool Equals(TsTimeStamp other) => UnixMilliseconds == other.UnixMilliseconds;

        public override int GetHashCode() => UnixMilliseconds.GetHashCode();

        public override string ToString() => UtcDateTime.ToString();
    }
}
