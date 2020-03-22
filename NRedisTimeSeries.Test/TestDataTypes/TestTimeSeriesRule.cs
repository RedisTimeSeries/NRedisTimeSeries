using System;
using NRedisTimeSeries.DataTypes;
using Xunit;

namespace NRedisTimeSeries.Test.TestDataTypes
{
    public class TestTimeSeriesRule
    {
        public TestTimeSeriesRule() { }

        [Fact]
        public void TestRuleConstructor()
        {
            TimeSeriesRule rule = new TimeSeriesRule("key", 50, Aggregation.AVG);
            Assert.Equal("key", rule.DestKey);
            Assert.Equal(Aggregation.AVG, rule.Aggregation);
            Assert.Equal(50, rule.TimeBucket);
        }

        [Fact]
        public void TestRuleEquals()
        {
            TimeSeriesRule rule = new TimeSeriesRule("key", 50, Aggregation.AVG);

            TimeSeriesRule rule1 = new TimeSeriesRule("key", 50, Aggregation.AVG);
            TimeSeriesRule rule2 = new TimeSeriesRule("key2", 50, Aggregation.AVG);
            TimeSeriesRule rule3 = new TimeSeriesRule("key", 51, Aggregation.AVG);
            TimeSeriesRule rule4 = new TimeSeriesRule("key", 50, Aggregation.COUNT);

            Assert.Equal(rule, rule1);
            Assert.NotEqual(rule, rule2);
            Assert.NotEqual(rule, rule3);
            Assert.NotEqual(rule, rule4);
        }

        [Fact]
        public void TestRuleHashCode()
        {
            TimeSeriesRule rule = new TimeSeriesRule("key", 50, Aggregation.AVG);

            TimeSeriesRule rule1 = new TimeSeriesRule("key", 50, Aggregation.AVG);
            TimeSeriesRule rule2 = new TimeSeriesRule("key2", 50, Aggregation.AVG);
            TimeSeriesRule rule3 = new TimeSeriesRule("key", 51, Aggregation.AVG);
            TimeSeriesRule rule4 = new TimeSeriesRule("key", 50, Aggregation.COUNT);

            Assert.Equal(rule.GetHashCode(), rule1.GetHashCode());
            Assert.NotEqual(rule.GetHashCode(), rule2.GetHashCode());
            Assert.NotEqual(rule.GetHashCode(), rule3.GetHashCode());
            Assert.NotEqual(rule.GetHashCode(), rule4.GetHashCode());
        }
    }
}
