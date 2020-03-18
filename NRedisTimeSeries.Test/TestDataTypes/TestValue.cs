using System;
using Xunit;

namespace NRedisTimeSeries.Test
{
    public class TestValue
    {
        [Fact]
        public void TestValueConstructor()
        {
            Value value = new Value(1, 1.1);
            Assert.Equal(1, value.Time);
            Assert.Equal(1.1, value.Val);
        }

        [Fact]
        public void TestValueEqual()
        {
            Value value1 = new Value(1, 1.1);
            Value value1_1 = new Value(1, 1.1);
            Value value1_2 = new Value(2, 2.2);
            Assert.Equal(value1, value1_1);
            Assert.NotEqual(value1, value1_2);
        }

        [Fact]
        public void TestValueHashCode()
        {
            Value value1 = new Value(1, 1.1);
            Value value1_1 = new Value(1, 1.1);
            Value value1_2 = new Value(2, 2.2);
            Assert.Equal(value1.GetHashCode(), value1_1.GetHashCode());
            Assert.NotEqual(value1.GetHashCode(), value1_2.GetHashCode());
        }
    }
}
