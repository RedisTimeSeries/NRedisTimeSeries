using NRedisTimeSeries.Commands;
using System;

namespace NRedisTimeSeries.Extensions
{
<<<<<<< HEAD
    internal static class PolicyExtensions
=======
    internal static class DuplicatePolicyExtensions
>>>>>>> 034850760bf8c0dfd05815606798e9452333204e
    {
        public static string AsArg(this TsPolicy policy) => policy switch
        {
            TsPolicy.BLOCK => "BLOCK",
            TsPolicy.FIRST => "FIRST",
            TsPolicy.LAST => "LAST",
            TsPolicy.MIN => "MIN",
            TsPolicy.MAX => "MAX",
            TsPolicy.SUM => "SUM",
            _ => throw new ArgumentOutOfRangeException(nameof(policy), "Invalid policy type"),
        };

        public static TsPolicy AsPolicy(string policy) => policy switch
        {
            "BLOCK" => TsPolicy.BLOCK,
            "FIRST" => TsPolicy.FIRST,
            "LAST" => TsPolicy.LAST,
            "MIN" => TsPolicy.MIN,
            "MAX" => TsPolicy.MAX,
            "SUM" => TsPolicy.SUM,
            _ => throw new ArgumentOutOfRangeException(nameof(policy), $"Invalid policy type '{policy}'"),
        };
    }
}
