package com.readytalk.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Writer;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;

@Slf4j
public class StatsdMetricSerializer {

  private final Locale locale;
  private final Writer writer;
  private final String prefix;

  private boolean prependNewline = false;

  public StatsdMetricSerializer(Writer writer) {
    this(writer, Locale.US);
  }

  public StatsdMetricSerializer(Writer writer, Locale locale) {
    this(writer, locale, "");
  }

  public StatsdMetricSerializer(Writer writer, Locale locale, String prefix) {
    this.writer = writer;
    this.locale = locale;
    this.prefix = prefix;
  }

  public void processGauges(SortedMap<String, Gauge> gauges) {
    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      sendObj(entry.getKey() + ".count", StatType.GAUGE, entry.getValue().getValue());
    }
  }

  public void processCounters(SortedMap<String, Counter> counters) {
    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      sendObj(entry.getKey() + ".count", StatType.GAUGE, entry.getValue().getCount());
    }
  }

  public void processHistograms(SortedMap<String, Histogram> histograms) {
    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      processSampling(entry.getKey(), entry.getValue());
    }
  }

  public void processMeters(SortedMap<String, Meter> meters) {
    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      processMetered(entry.getKey(), entry.getValue());
    }
  }

  public void processTimers(SortedMap<String, Timer> timers) {
    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      processMetered(entry.getKey(), entry.getValue());
      processSampling(entry.getKey(), entry.getValue());
    }
  }

  private void processMetered(String name, Metered meter) {
    sendInt(name + ".count", StatType.GAUGE, meter.getCount());
    sendFloat(name + ".meanRate", StatType.TIMER, meter.getMeanRate());
    sendFloat(name + ".1MinuteRate", StatType.TIMER, meter.getOneMinuteRate());
    sendFloat(name + ".5MinuteRate", StatType.TIMER, meter.getFiveMinuteRate());
    sendFloat(name + ".15MinuteRate", StatType.TIMER, meter.getFiveMinuteRate());
  }

  private void processSampling(String name, Sampling sampling) {
    Snapshot snapshot = sampling.getSnapshot();
    sendFloat(name + ".min", StatType.TIMER, snapshot.getMin());
    sendFloat(name + ".max", StatType.TIMER, snapshot.getMax());
    sendFloat(name + ".mean", StatType.TIMER, snapshot.getMean());
    sendFloat(name + ".stddev", StatType.TIMER, snapshot.getStdDev());
    sendFloat(name + ".median", StatType.TIMER, snapshot.getMedian());
    sendFloat(name + ".75percentile", StatType.TIMER, snapshot.get75thPercentile());
    sendFloat(name + ".95percentile", StatType.TIMER, snapshot.get95thPercentile());
    sendFloat(name + ".98percentile", StatType.TIMER, snapshot.get98thPercentile());
    sendFloat(name + ".99percentile", StatType.TIMER, snapshot.get99thPercentile());
    sendFloat(name + ".999percentile", StatType.TIMER, snapshot.get999thPercentile());
    sendObj(name + ".sampleCount", StatType.GAUGE, snapshot.size());
  }

  protected void sendInt(String name, StatType statType, long value) {
    sendData(name, String.format(locale, "%d", value), statType);
  }

  protected void sendFloat(String name, StatType statType, double value) {
    sendData(name, String.format(locale, "%2.2f", value), statType);
  }

  protected void sendObj(String name, StatType statType, Object value) {
    sendData(name, String.format(locale, "%s", value), statType);
  }

  protected String sanitizeString(String s) {
    return s.replace(' ', '-');
  }

  protected void sendData(String name, String value, StatType statType) {
    try {
      if (prependNewline) {
        writer.write("\n");
      }
      if (!prefix.isEmpty()) {
        writer.write(prefix);
      }
      writer.write(sanitizeString(name));
      writer.write(":");
      writer.write(value);
      writer.write("|");
      writer.write(statType.getStatsdUnit());
      prependNewline = true;
      writer.flush();
    } catch (IOException e) {
      log.error("Error serializing metric: ", e);
    }
  }

  private static enum StatType {
    COUNTER("c"),
    TIMER("ms"),
    GAUGE("g");

    private final String statsdUnit;

    StatType(String statsdUnit) {
      this.statsdUnit = statsdUnit;
    }

    public String getStatsdUnit() {
      return statsdUnit;
    }
  }
}
