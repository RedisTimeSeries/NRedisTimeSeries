namespace NRedisTimeSeries.Commands
{
    internal class CommandArgs
    {
        public static string RETENTION => "RETENTION";
        public static string LABELS => "LABELS";
        public static string UNCOMPRESSED => "UNCOMPRESSED";
        public static string COUNT => "COUNT";
        public static string AGGREGATION => "AGGREGATION";
        public static string FILTER => "FILTER";
        public static string WITHLABELS => "WITHLABELS";
        public static string TIMESTAMP => "TIMESTAMP";
        public static string CHUNK_SIZE => "CHUNK_SIZE";
        public static string DUPLICATE_POLICY => "DUPLICATE_POLICY";
        public static string ON_DUPLICATE => "ON_DUPLICATE";
        public static string GROPUBY => "GROUPBY";
        public static string REDUCE => "REDUCE";
    }
}
