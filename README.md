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
//Simple example
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();
db.TimeSeriesCreate("test", labels:new TimeSeriesLabel("Time", "Series"));
db.TimeSeriesAdd("test", 1, 1.12);
db.TimeSeriesAdd("test", 2, 1.12);
db.TimeSeriesGet("test");
db.TimeSeriesIncrBy("test", 1);
db.TimeSeriesRange("test", "-", "+");
db.TimeSeriesRange("test", "-", "+", aggregation: TsAggregation.Avg, timeBucket: 10);
TimeSeriesInformation info = db.TimeSeriesInfo("test");               

//Example with rules
db.TimeSeriesCreate("source", retentionTime: 40)
db.TimeSeriesCreate("sumRule")
db.TimeSeriesCreate("avgRule")
TimeSeriesRule rule1 = new TimeSeriesRule("sumRule", 20, TsAggregation.Sum);
db.TimeSeriesCreateRule("source", rule1);
TimeSeriesRule rule2 = new TimeSeriesRule("avgRule", 15, TsAggregation.Avg);
db.TimeSeriesCreateRule("source", rule2);
db.TimeSeriesAdd("source", "*", 1);
db.TimeSeriesAdd("source", "*", 2);
db.TimeSeriesAdd("source", "*", 3);
db.TimeSeriesGet("source");
db.TimeSeriesGet("sumRule");
db.TimeSeriesGet("avgRule");
```

## Further notes on back-filling time series

Since [RedisTimeSeries 1.4](https://github.com/RedisTimeSeries/RedisTimeSeries/releases/tag/v1.4.5) we've added the ability to back-fill time series, with different duplicate policies. 

The default behavior is to block updates to the same timestamp, and you can control it via the `duplicate_policy` argument. You can check in detail the [duplicate policy documentation](https://oss.redislabs.com/redistimeseries/configuration/#duplicate_policy).

Bellow you can find an example of the `LAST` duplicate policy, in which we override duplicate timestamps with the latest value:

```C#
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase();
db.TimeSeriesCreate("last-upsert", labels: new TimeSeriesLabel("Time", "Series"), duplicatePolicy: TsDuplicatePolicy.LAST)
db.TimeSeriesAdd("last-upsert", 1, 10.0)
db.TimeSeriesAdd("last-upsert", 1, 5.0)
// should output [(1, 5.0)]
System.Console.WriteLine(db.TimeSeriesRange("last-upsert", "-", "+"))
```

See the [Example project](NRedisTimeSeries.Example) for commands reference
