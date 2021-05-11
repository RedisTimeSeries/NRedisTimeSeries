[![license](https://img.shields.io/github/license/RedisTimeSeries/NRedisTimeSeries.svg)](https://github.com/RedisTimeSeries/NRedisTimeSeries)
[![CircleCI](https://circleci.com/gh/RedisTimeSeries/NRedisTimeSeries/tree/master.svg?style=svg)](https://circleci.com/gh/RedisTimeSeries/NRedisTimeSeries/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RedisTimeSeries/NRedisTimeSeries.svg)](https://github.com/RedisTimeSeries/NRedisTimeSeries/releases/latest)
[![Codecov](https://codecov.io/gh/RedisTimeSeries/NRedisTimeSeries/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisTimeSeries/NRedisTimeSeries)
[![Known Vulnerabilities](https://snyk.io/test/github/RedisTimeSeries/NRedisTimeSeries/badge.svg?targetFile=NRedisTimeSeries/NRedisTimeSeries.csproj)](https://snyk.io/test/github/RedisTimeSeries/NRedisTimeSeries?targetFile=NRedisTimeSeries/NRedisTimeSeries.csproj)
[![StackExchange.Redis](https://img.shields.io/nuget/v/NRedisTimeSeries.svg)](https://www.nuget.org/packages/NRedisTimeSeries/)

# NRedisTimeSeries
[![Forum](https://img.shields.io/badge/Forum-RedisTimeSeries-blue)](https://forum.redislabs.com/c/modules/redistimeseries)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/KExRgMb)

.Net Client for RedisTimeSeries


## API
The complete documentation of RedisTimeSeries's commands can be found at [RedisTimeSeries's website](http://redistimeseries.io/).

## Usage example

```C#
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();

// Create 
var label = new TimeSeriesLabel("Time", "Series");
db.TimeSeriesCreate("test", retentionTime: 5000, labels: new List<TimeSeriesLabel> { label }, duplicatePolicy: TsDuplicatePolicy.MAX);

// Alter
label = new TimeSeriesLabel("Alter", "label");
db.TimeSeriesAlter("test", retentionTime: 0, labels: new List<TimeSeriesLabel> { label });

// Add
db.TimeSeriesAdd("test", 1, 1.12);
db.TimeSeriesAdd("test", 1, 1.13, duplicatePolicy: TsDuplicatePolicy.LAST);

// MAdd
string[] keys = { "system_time_ts", "datetime_ts", "long_ts" };
var sequence = new List<(string, TimeStamp, double)>(keys.Length);
sequence.Add(("test", "*", 0.0));
sequence.Add(("test", DateTime.UtcNow, 0.0));
sequence.Add(("test", 1, 1.0));
db.TimeSeriesMAdd(sequence);

// Rule
db.TimeSeriesCreate("sumRule");
TimeSeriesRule rule = new TimeSeriesRule("sumRule", 20, TsAggregation.Sum);
db.TimeSeriesCreateRule("test", rule);
db.TimeSeriesAdd("test", "*", 1);
db.TimeSeriesAdd("test", "*", 2);
db.TimeSeriesDeleteRule("test", "sumRule");
db.KeyDelete("sumRule");

// Range
db.TimeSeriesRange("test", "-", "+");
db.TimeSeriesRange("test", "-", "+", aggregation: TsAggregation.Avg, timeBucket: 10);

// Get
db.TimeSeriesGet("test");

// Info
TimeSeriesInformation info = db.TimeSeriesInfo("test");               

// DEL
db.KeyDelete("test");
```

## Further notes on back-filling time series

Since [RedisTimeSeries 1.4](https://github.com/RedisTimeSeries/RedisTimeSeries/releases/tag/v1.4.5) we've added the ability to back-fill time series, with different duplicate policies. 

The default behavior is to block updates to the same timestamp, and you can control it via the `duplicatePolicy` argument. You can check in detail the [duplicate policy documentation](https://oss.redislabs.com/redistimeseries/configuration/#duplicate_policy).


See the [Example project](NRedisTimeSeries.Example) for commands reference