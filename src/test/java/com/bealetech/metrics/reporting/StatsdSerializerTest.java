package com.bealetech.metrics.reporting;

import org.junit.Before;
import org.junit.Test;

import java.io.StringWriter;

public class StatsdSerializerTest {
	private static final String FAKE_METRIC_NAME = "foo.bar";
	private static final int INT_VAL = 3;

	private StringWriter writer;
	private StatsdSerializer serializer;

	@Before
	public void init() {
		writer = new StringWriter();
	}

	private void createSerializerEmptyPrefix() {
		serializer = new StatsdSerializer("", writer);
	}

	@Test
	public void testSendInt() {
		createSerializerEmptyPrefix();
		serializer.sendInt(FAKE_METRIC_NAME, StatType.COUNTER, INT_VAL);
		//TODO verify
	}
}
