using System;
using NRedisTimeSeries.DataTypes;
using Xunit;

namespace NRedisTimeSeries.Test.TestDataTypes
{
    public class TestLabel
    {
        [Fact]
        public void TestLabelConstructor()
        {
            TimeSeriesLabel label = new TimeSeriesLabel("a", "b");
            Assert.Equal("a", label.Key);
            Assert.Equal("b", label.Value);
        }


        [Fact]
        public void TestLbaelEquals()
        {
            TimeSeriesLabel label_ab = new TimeSeriesLabel("a", "b");
            TimeSeriesLabel label1 = new TimeSeriesLabel("a", "b");
            TimeSeriesLabel label2 = new TimeSeriesLabel("a", "c");
            TimeSeriesLabel label3 = new TimeSeriesLabel("c", "b");

            Assert.Equal(label_ab, label1);
            Assert.NotEqual(label_ab, label2);
            Assert.NotEqual(label_ab, label3);
        }
       
    }
}
