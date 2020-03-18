using System;
namespace NRedisTimeSeries.DataTypes
{
    public class Aggregation
    {
        private Aggregation(string value) => this.value = value;

        private readonly string value;

        public static Aggregation AVG { get { return new Aggregation("avg"); } }
        public static Aggregation SUM { get { return new Aggregation("sum"); } }
        public static Aggregation MIN { get { return new Aggregation("min"); } }
        public static Aggregation MAX { get { return new Aggregation("max"); } }
        public static Aggregation RANGE { get { return new Aggregation("range"); } }
        public static Aggregation COUNT { get { return new Aggregation("count"); } }
        public static Aggregation FIRST { get { return new Aggregation("first"); } }
        public static Aggregation LAST { get { return new Aggregation("last"); } }
        public static Aggregation STDP { get { return new Aggregation("std.p"); } }
        public static Aggregation STDS { get { return new Aggregation("std.s"); } }
        public static Aggregation VARP { get { return new Aggregation("var.p"); } }
        public static Aggregation VARS { get { return new Aggregation("var.s"); } }

        public static implicit operator string(Aggregation aggregation) => aggregation.value;

    }
}
