using System;
using Xunit;
namespace NRedisTimeSeries.Test
{
    public class TestTimeStamp
    {
        private TimeStamp ImplicitCast(TimeStamp ts)
        {
            return ts;
        }

        [Fact]
        public void TestTimeStampImplicitCast()
        {
            TimeStamp ts = ImplicitCast(1);
            Assert.Equal<long>(1, ts);

            ts = ImplicitCast("+");
            Assert.Equal("+", ts);

            ts = ImplicitCast("*");
            Assert.Equal("*", ts);

            ts = ImplicitCast("-");
            Assert.Equal("-", ts);

            var ex = Assert.Throws<NotSupportedException>(()=>ImplicitCast("hi"));
            Assert.Equal("The string hi cannot be used", ex.Message);

            DateTime now = DateTime.Now;
            ts = ImplicitCast(now);
            Assert.Equal<DateTime>(now, ts);

        }
    }
}
