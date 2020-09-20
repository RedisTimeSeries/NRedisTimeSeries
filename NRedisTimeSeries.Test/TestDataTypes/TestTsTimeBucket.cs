using NRedisTimeSeries.DataTypes;
using System;
using Xunit;
namespace NRedisTimeSeries.Test
{
    public class TestTsTimeBucket
    {
        [Fact]
        public void TestConstruction()
        {
            // Test default and empty constructors
            var tsTimeBucketDefault = default(TsTimeBucket);
            Assert.Equal(new TsTimeBucket(), tsTimeBucketDefault);

            // Test DateTime constructor
            var now = DateTime.UtcNow;
            var nowPlus1Day = now.AddDays(1);
            var tsTimeBucketDateTime = new TsTimeBucket(now, nowPlus1Day);
            Assert.True(tsTimeBucketDateTime.UnixMilliseconds > 0);

            // Test UnixMs long constructor
            var tsTimeBucketsUnixMs = new TsTimeBucket(1000);
            Assert.Equal(1000, tsTimeBucketsUnixMs.UnixMilliseconds);
        }

        [Fact]
        public void TestRanges()
        {
            var tsMin = new TsTimeBucket(DateTime.UnixEpoch.AddMilliseconds(1), DateTime.UnixEpoch.AddMilliseconds(1000));
            Assert.Equal(999, tsMin.UnixMilliseconds);

            var tsMax = new TsTimeBucket(DateTime.UnixEpoch.AddMilliseconds(1), DateTime.MaxValue.ToUniversalTime());
            Assert.Equal(TsTimeBucket.MaxValue, tsMax);

            var tsMinMs = new TsTimeBucket(1);
            Assert.Equal(TsTimeBucket.MinValue, tsMinMs);

            var tsMaxMs = new TsTimeBucket(253_402_271_999_998);
            Assert.Equal(TsTimeBucket.MaxValue, tsMaxMs);
        }

        [Fact]
        public void TestExceptions()
        {
            var ex0 = Assert.Throws<ArgumentException>(() => new TsTimeBucket(new TsTimeStamp(0), new TsTimeStamp(0)));
            Assert.Equal("The time bucket must be 1ms or greater", ex0.Message);

            var ex1 = Assert.Throws<ArgumentOutOfRangeException>(() => new TsTimeBucket(0));
            Assert.Equal("Must be 1ms or greater (Parameter 'unixMilliseconds')", ex1.Message);

            var ex2 = Assert.Throws<ArgumentOutOfRangeException>(() => new TsTimeBucket(-1));
            Assert.Equal($"Must be {1}ms or greater (Parameter 'unixMilliseconds')", ex2.Message);

            var ex3 = Assert.Throws<ArgumentOutOfRangeException>(() => new TsTimeBucket(long.MaxValue));
            Assert.Equal($"Must be {253_402_271_999_998}ms or less (Parameter 'unixMilliseconds')", ex3.Message);

            var ex4 = Assert.Throws<ArgumentOutOfRangeException>(() => new TsTimeBucket(DateTime.MinValue.ToUniversalTime(), DateTime.MaxValue.ToUniversalTime()));
            Assert.Equal($"Must be Unix Epoch time or greater (Parameter 'dateTime')", ex4.Message);
        }
    }
}
