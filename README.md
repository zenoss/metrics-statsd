# metrics-statsd

Statsd reporter for codahale/metrics.

## Quick Start

```java
MetricsRegistry registry = new MetricsRegistry();
StatsdReporter reporter = new StatsdReporter(registry, "statsd.example.com", 8125);
reporter.start(15, TimeUnit.SECONDS);
```

## Dependency

```xml
<dependencies>
    <dependency>
        <groupId>com.readytalk</groupId>
        <artifactId>metrics3-statsd</artifactId>
        <version>${metrics-statsd.version}</version>
    </dependency>
</dependencies>
```
# License

Copyright (c) 2012-2013 Sean Laurent

Published under Apache Software License 2.0, see LICENSE

This is based off of Sean Laurent's [metrics-statsd](https://github.com/organicveggie/metrics-statsd) and the graphite module of [Coda Hale's Metrics](https://github.com/codahale/metrics)
