package com.readytalk.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.DatagramSocket;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class StatsdReporter extends ScheduledReporter {

  private UdpSocketProvider socketProvider = null;

  /**
   * @param registry the {@link com.codahale.metrics.MetricRegistry} containing the metrics this
   *                 reporter will report
   * @param name     the reporter's name
   * @param filter   the filter for which metrics to report
   */
  public StatsdReporter(MetricRegistry registry, String name, MetricFilter filter,
                        TimeUnit rateUnit, TimeUnit durationUnit, String host, int port) {
    super(registry, name, filter, rateUnit, durationUnit);
    this.socketProvider = new DefaultUdpSocketProvider(host, port);
  }

  /**
   * @param registry the {@link com.codahale.metrics.MetricRegistry} containing the metrics this
   *                 reporter will report
   * @param name     the reporter's name
   */
  public StatsdReporter(MetricRegistry registry, String name, String host, int port) {
    this(registry, name, null, TimeUnit.MINUTES, TimeUnit.SECONDS, host, port);
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    DatagramSocket socket = null;

    ByteArrayOutputStream outputData = new ByteArrayOutputStream();
    Writer writer = new BufferedWriter(new OutputStreamWriter(outputData));

    StatsdMetricSerializer metricSerializer = new StatsdMetricSerializer(writer);
    metricSerializer.processGauges(gauges);
    metricSerializer.processCounters(counters);
    metricSerializer.processHistograms(histograms);
    metricSerializer.processMeters(meters);
    metricSerializer.processTimers(timers);

    byte[] data = outputData.toByteArray();

    try {
      socket = socketProvider.newSocket();
      socket.send(socketProvider.newPacket(data));
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("Error writing to Statsd", e);
      } else {
        log.warn("Error writing to Statsd: {}", e.getMessage());
      }
      if (writer != null) {
        try {
          writer.flush();
        } catch (IOException e1) {
          log.error("Error while flushing writer:", e1);
        }
      }
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }

  public UdpSocketProvider getSocketProvider() {
    return socketProvider;
  }

  public void setSocketProvider(UdpSocketProvider socketProvider) {
    this.socketProvider = socketProvider;
  }

}
