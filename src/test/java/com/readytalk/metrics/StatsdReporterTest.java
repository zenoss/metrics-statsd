package com.readytalk.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.DatagramSocket;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatsdReporterTest {

  public static final double RATE_15_MINUTE = 15.15;
  public static final double RATE_5_MINUTE = 5.5;
  public static final double RATE_1_MINUTE = 1.1;
  public static final double MEAN = 1.1d;
  public static final double MEDIAN = 2.2d;
  public static final double STDDEV = 3.3d;
  public static final double PCT_75_TH = 4.4d;
  public static final double PCT_95_TH = 5.5d;
  public static final double PCT_98_TH = 6.6d;
  public static final double PCT_99_TH = 7.7d;
  public static final double PCT_999_TH = 8.8d;
  public static final int HISTOGRAM_SAMPLE_SIZE = 1000;
  public static final long HISTOGRAM_COUNT = 42l;
  public static final int TIMER_SIZE = 1000;

  @Test
  public void testReport() throws IOException {
    StatsdReporter reporter = new StatsdReporter(new MetricRegistry(), "test-statsd-reporter", "bogus.readytalk.com", 8125);
    UdpSocketProvider socketProvider = mock(DefaultUdpSocketProvider.class);
    when(socketProvider.newSocket()).thenReturn(mock(DatagramSocket.class));
    when(socketProvider.newPacket(any(byte[].class))).thenCallRealMethod();

    reporter.setSocketProvider(socketProvider);
    reporter.report(guages(), counters(), histograms(), meters(), timers());

    ArgumentCaptor<byte[]> bufferCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(socketProvider).newPacket(bufferCaptor.capture());

    byte[] buffer = bufferCaptor.getValue();
    String metrics = new String(buffer);

    String expectedMetrics =
        "guage.1.count:42|g\n"
            + "guage.2.count:awesome|g\n"
            + "counter.1.count:9223372036854775807|g\n"
            + "counter.2.count:-9223372036854775808|g\n"
            + "histogram.1.min:-9223372036854776000.00|ms\n"
            + "histogram.1.max:9223372036854776000.00|ms\n"
            + "histogram.1.mean:1.10|ms\n"
            + "histogram.1.stddev:3.30|ms\n"
            + "histogram.1.median:2.20|ms\n"
            + "histogram.1.75percentile:4.40|ms\n"
            + "histogram.1.95percentile:5.50|ms\n"
            + "histogram.1.98percentile:6.60|ms\n"
            + "histogram.1.99percentile:7.70|ms\n"
            + "histogram.1.999percentile:8.80|ms\n"
            + "histogram.1.sampleCount:1000|g\n"
            + "meter.1.count:9223372036854775807|g\n"
            + "meter.1.meanRate:0.00|ms\n"
            + "meter.1.1MinuteRate:1.10|ms\n"
            + "meter.1.5MinuteRate:5.50|ms\n"
            + "meter.1.15MinuteRate:5.50|ms\n"
            + "timer.1.count:9223372036854775807|g\n"
            + "timer.1.meanRate:0.00|ms\n"
            + "timer.1.1MinuteRate:1.10|ms\n"
            + "timer.1.5MinuteRate:5.50|ms\n"
            + "timer.1.15MinuteRate:5.50|ms\n"
            + "timer.1.min:-9223372036854776000.00|ms\n"
            + "timer.1.max:9223372036854776000.00|ms\n"
            + "timer.1.mean:1.10|ms\n"
            + "timer.1.stddev:3.30|ms\n"
            + "timer.1.median:2.20|ms\n"
            + "timer.1.75percentile:4.40|ms\n"
            + "timer.1.95percentile:5.50|ms\n"
            + "timer.1.98percentile:6.60|ms\n"
            + "timer.1.99percentile:7.70|ms\n"
            + "timer.1.999percentile:8.80|ms\n"
            + "timer.1.sampleCount:1000|g";

    Assert.assertEquals(expectedMetrics, metrics);
  }

  private SortedMap<String, Gauge> guages() {
    TreeMap<String, Gauge> guages = new TreeMap<String, Gauge>();
    guages.put("guage.1", mockGauge(42));
    guages.put("guage.2", mockGauge("awesome"));
    return guages;
  }

  private Gauge mockGauge(Object value) {
    Gauge gauge = mock(Gauge.class);
    when(gauge.getValue()).thenReturn(value);
    return gauge;
  }

  private SortedMap<String, Counter> counters() {
    TreeMap<String, Counter> counters = new TreeMap<String, Counter>();
    counters.put("counter.1", mockCounter(Long.MAX_VALUE));
    counters.put("counter.2", mockCounter(Long.MIN_VALUE));
    return counters;
  }

  private SortedMap<String, Histogram> histograms() {
    TreeMap<String, Histogram> histograms = new TreeMap<String, Histogram>();
    Snapshot snapshot = mockSnapshot(HISTOGRAM_SAMPLE_SIZE, Long.MIN_VALUE, Long.MAX_VALUE,
        MEAN, MEDIAN, STDDEV, PCT_75_TH, PCT_95_TH, PCT_98_TH, PCT_99_TH, PCT_999_TH);
    histograms.put("histogram.1", mockHistogram(HISTOGRAM_COUNT, snapshot));
    return histograms;
  }

  private SortedMap<String, Meter> meters() {
    TreeMap<String, Meter> meters = new TreeMap<String, Meter>();
    meters.put("meter.1", mockMeter(Long.MAX_VALUE));
    return meters;
  }

  private Meter mockMeter(Long count) {
    Meter meter = mock(Meter.class);
    mockMetered(meter, count);
    return meter;
  }

  private SortedMap<String, Timer> timers() {
    TreeMap<String, Timer> timers = new TreeMap<String, Timer>();
    timers.put("timer.1", mockTimer(Long.MAX_VALUE));
    return timers;
  }

  private Timer mockTimer(Long count) {
    Snapshot snapshot = mockSnapshot(TIMER_SIZE, Long.MIN_VALUE, Long.MAX_VALUE,
    MEAN, MEDIAN, STDDEV, PCT_75_TH, PCT_95_TH, PCT_98_TH, PCT_99_TH, PCT_999_TH);
    Timer timer = mock(Timer.class);
    when(timer.getSnapshot()).thenReturn(snapshot);
    mockMetered(timer, count);
    return timer;
  }

  private void mockMetered(Metered metered, Long count) {
    when(metered.getCount()).thenReturn(count);
    when(metered.getFifteenMinuteRate()).thenReturn(RATE_15_MINUTE);
    when(metered.getFiveMinuteRate()).thenReturn(RATE_5_MINUTE);
    when(metered.getOneMinuteRate()).thenReturn(RATE_1_MINUTE);
  }

  private Counter mockCounter(long count) {
    Counter counter = mock(Counter.class);
    when(counter.getCount()).thenReturn(count);
    return counter;
  }

  private Histogram mockHistogram(Long count, Snapshot snapshot) {
    Histogram histogram = mock(Histogram.class);
    when(histogram.getCount()).thenReturn(count);
    when(histogram.getSnapshot()).thenReturn(snapshot);
    return histogram;
  }

  private Snapshot mockSnapshot(int size, long min, long max, double mean, double median,
                                double stddev, double pct75th, double pct95th,
                                double pct98th, double pct99th, double pct999th) {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.getMin()).thenReturn(min);
    when(snapshot.getMax()).thenReturn(max);
    when(snapshot.getMean()).thenReturn(mean);
    when(snapshot.getMedian()).thenReturn(median);
    when(snapshot.getStdDev()).thenReturn(stddev);
    when(snapshot.get75thPercentile()).thenReturn(pct75th);
    when(snapshot.get95thPercentile()).thenReturn(pct95th);
    when(snapshot.get98thPercentile()).thenReturn(pct98th);
    when(snapshot.get99thPercentile()).thenReturn(pct99th);
    when(snapshot.get999thPercentile()).thenReturn(pct999th);
    when(snapshot.size()).thenReturn(size);
    return snapshot;
  }

}