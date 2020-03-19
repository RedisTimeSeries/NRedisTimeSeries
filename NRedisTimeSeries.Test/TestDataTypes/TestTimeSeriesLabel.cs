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
            Label label = new Label("a", "b");
            Assert.Equal("a", label.Key);
            Assert.Equal("b", label.Value);
        }


        [Fact]
        public void TestLbaelEquals()
        {
            Label label_ab = new Label("a", "b");
            Label label1 = new Label("a", "b");
            Label label2 = new Label("a", "c");
            Label label3 = new Label("c", "b");

            Assert.Equal(label_ab, label1);
            Assert.NotEqual(label_ab, label2);
            Assert.NotEqual(label_ab, label3);
        }
       
    }
}
