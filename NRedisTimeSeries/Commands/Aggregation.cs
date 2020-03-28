using System;
using System.Collections.Generic;

namespace NRedisTimeSeries.Commands
{
    /// <summary>
    /// A wrapper class around aggregation name. Each static member of the class is a wrapper around RedisTimerSeries aggregation.
    /// This class can be cast to and from string.
    /// </summary>
    public class Aggregation
    {
        private Aggregation(string name) => this.Name = name;

        /// <summary>
        /// String property with the aggregation name.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// AVG Aggregation
        /// </summary>
        public static Aggregation AVG { get { return new Aggregation("avg"); } }
        /// <summary>
        /// SUM Aggregation
        /// </summary>
        public static Aggregation SUM { get { return new Aggregation("sum"); } }
        /// <summary>
        /// MIN Aggregation
        /// </summary>
        public static Aggregation MIN { get { return new Aggregation("min"); } }
        /// <summary>
        /// MAX Aggregation
        /// </summary>
        public static Aggregation MAX { get { return new Aggregation("max"); } }
        /// <summary>
        /// RANGE Aggregation
        /// </summary>
        public static Aggregation RANGE { get { return new Aggregation("range"); } }
        /// <summary>
        /// COUNT Aggregation
        /// </summary>
        public static Aggregation COUNT { get { return new Aggregation("count"); } }
        /// <summary>
        /// FIRST Aggregarion
        /// </summary>
        public static Aggregation FIRST { get { return new Aggregation("first"); } }
        /// <summary>
        /// LAST Aggregation
        /// </summary>
        public static Aggregation LAST { get { return new Aggregation("last"); } }
        /// <summary>
        /// STD.P Aggregation
        /// </summary>
        public static Aggregation STDP { get { return new Aggregation("std.p"); } }
        /// <summary>
        /// STD.S Aggregation
        /// </summary>
        public static Aggregation STDS { get { return new Aggregation("std.s"); } }
        /// <summary>
        /// VAR.P Aggregation
        /// </summary>
        public static Aggregation VARP { get { return new Aggregation("var.p"); } }
        /// <summary>
        /// VAR.S Aggregation
        /// </summary>
        public static Aggregation VARS { get { return new Aggregation("var.s"); } }

        /// <summary>
        /// Implicit cast to string.
        /// </summary>
        /// <param name="aggregation">Aggregation object</param>
        public static implicit operator string(Aggregation aggregation) => aggregation.Name;

        /// <summary>
        /// Implicit cast from string.
        /// </summary>
        /// <param name="s">string</param>
        public static implicit operator Aggregation(string s) => new Aggregation(s);

        /// <summary>
        /// Equality of Aggregation objects. Case Insensitive for the Name property string.
        /// </summary>
        /// <param name="obj">Object to compare</param>
        /// <returns>If two Aggregation objects are equal.</returns>
        public override bool Equals(object obj)
        {
            return obj is Aggregation aggregation &&
                   Name.Equals(aggregation.Name, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Aggregation object hash code.
        /// </summary>
        /// <returns>Aggregation object hash code.</returns>
        public override int GetHashCode()
        {
            return 539060726 + EqualityComparer<string>.Default.GetHashCode(Name);
        }
    }
}
