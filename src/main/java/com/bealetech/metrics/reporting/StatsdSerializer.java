package com.bealetech.metrics.reporting;

import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.util.Locale;

public class StatsdSerializer {
	private static final Logger LOG = LoggerFactory.getLogger(StatsdSerializer.class);

	private final Locale locale;
	private final String prefix;
	private final Writer writer;

	private boolean prependNewline;

	public StatsdSerializer(String metricPrefix, Writer writer) {
		this.locale = Locale.US;
		this.prefix = metricPrefix;
		this.writer = writer;
		this.prependNewline = false;
	}

	protected void sendSummarizable(String sanitizedName, Summarizable metric) {
		sendFloat(sanitizedName + ".min", StatType.TIMER, metric.min());
		sendFloat(sanitizedName + ".max", StatType.TIMER, metric.max());
		sendFloat(sanitizedName + ".mean", StatType.TIMER, metric.mean());
		sendFloat(sanitizedName + ".stddev", StatType.TIMER, metric.stdDev());
	}

	protected void sendSampling(String sanitizedName, Sampling metric) {
		final Snapshot snapshot = metric.getSnapshot();
		sendFloat(sanitizedName + ".median", StatType.TIMER, snapshot.getMedian());
		sendFloat(sanitizedName + ".75percentile", StatType.TIMER, snapshot.get75thPercentile());
		sendFloat(sanitizedName + ".95percentile", StatType.TIMER, snapshot.get95thPercentile());
		sendFloat(sanitizedName + ".98percentile", StatType.TIMER, snapshot.get98thPercentile());
		sendFloat(sanitizedName + ".99percentile", StatType.TIMER, snapshot.get99thPercentile());
		sendFloat(sanitizedName + ".999percentile", StatType.TIMER, snapshot.get999thPercentile());
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

	protected String sanitizeName(MetricName name) {
		final StringBuilder sb = new StringBuilder()
				.append(name.getGroup())
				.append('.')
				.append(name.getType())
				.append('.');
		if (name.hasScope()) {
			sb.append(name.getScope())
					.append('.');
		}
		return sb.append(name.getName()).toString();
	}

	protected String sanitizeString(String s) {
		return s.replace(' ', '-');
	}

	protected void sendData(String name, String value, StatType statType) {
		String statTypeStr = "";
		switch (statType) {
			case COUNTER:
				statTypeStr = "c";
				break;
			case GAUGE:
				statTypeStr = "g";
				break;
			case TIMER:
				statTypeStr = "ms";
				break;
		}

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
			writer.write(statTypeStr);
			prependNewline = true;
			writer.flush();
		} catch (IOException e) {
			LOG.error("Error sending to StatsD:", e);
		}
	}
}
