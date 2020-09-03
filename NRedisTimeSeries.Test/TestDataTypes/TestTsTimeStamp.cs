using NRedisTimeSeries.DataTypes;
using System;
using Xunit;
namespace NRedisTimeSeries.Test
{
    public class TestTsTimeStamp
    {
        [Fact]
        public void TestConstruction()
        {
            // Test default and empty constructors
            var tsDefault = default(TsTimeStamp);
            Assert.Equal(new TsTimeStamp(), tsDefault);

            // Test DateTime constructor
            var now = DateTime.UtcNow;
            var tsDateTime = new TsTimeStamp(now);
            Assert.True(tsDateTime.UnixMilliseconds > 0);

            // Test UnixMs long constructor
            var tsUnixMs = new TsTimeStamp(1000);
            Assert.Equal(1000, tsUnixMs.UnixMilliseconds);
        }

        [Fact]
        public void TestRanges()
        {
            var tsMin = new TsTimeStamp(DateTime.UnixEpoch);
            Assert.Equal(TsTimeStamp.MinValue, tsMin);

            var tsMax = new TsTimeStamp(DateTime.MaxValue.ToUniversalTime());
            Assert.Equal(TsTimeStamp.MaxValue, tsMax);

            var tsMinMs = new TsTimeStamp(0);
            Assert.Equal(TsTimeStamp.MinValue, tsMinMs);

            var tsMaxMs = new TsTimeStamp(253_402_271_999_999);
            Assert.Equal(TsTimeStamp.MaxValue, tsMaxMs);
        }

        [Fact]
        public void TestEquality()
        {
            var utcNow = DateTime.UtcNow;
            var unixMsNow = new DateTimeOffset(utcNow).ToUnixTimeMilliseconds();
            Assert.Equal(new TsTimeStamp(utcNow), new TsTimeStamp(unixMsNow));
        }

        [Fact]
        public void TestImplicitConversion()
        {
            TsTimeStamp tsMax = DateTime.MaxValue.ToUniversalTime();
            Assert.Equal(TsTimeStamp.MaxValue, tsMax);

            DateTime dtMax = tsMax;
            Assert.Equal((DateTime)TsTimeStamp.MaxValue, dtMax);
        }

        [Fact]
        public void TestExceptions()
        {
            var ex1 = Assert.Throws<ArgumentException>(() => new TsTimeStamp(DateTime.MaxValue));
            Assert.Equal("DateTime Kind must be UTC (Parameter 'dateTime')", ex1.Message);

            var ex2 = Assert.Throws<ArgumentOutOfRangeException>(() => new TsTimeStamp(-1));
            Assert.Equal($"Must be {0}ms or greater (Parameter 'unixMilliseconds')", ex2.Message);

            var ex3 = Assert.Throws<ArgumentOutOfRangeException>(() => new TsTimeStamp(long.MaxValue));
            Assert.Equal($"Must be {253_402_271_999_999}ms or less (Parameter 'unixMilliseconds')", ex3.Message);

            var ex4 = Assert.Throws<ArgumentOutOfRangeException>(() => new TsTimeStamp(DateTime.MinValue.ToUniversalTime()));
            Assert.Equal($"Must be Unix Epoch time or greater (Parameter 'dateTime')", ex4.Message);
        }
    }
}
