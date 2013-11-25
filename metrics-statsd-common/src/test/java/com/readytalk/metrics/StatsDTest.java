/**
 * Copyright (C) 2013 metrics-statsd contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.readytalk.metrics;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatsDTest {
  private final DatagramSocketFactory socketFactory = mock(DatagramSocketFactory.class);
  private final InetSocketAddress address = new InetSocketAddress("example.com", 1234);
  private final StatsD statsD = new StatsD(address, socketFactory);

  private final DatagramSocket socket = mock(DatagramSocket.class);

  private final ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);
  private final ArgumentCaptor<InetSocketAddress> addressCaptor = ArgumentCaptor.forClass(InetSocketAddress.class);

  @Before
  public void setUp() throws Exception {
    when(socketFactory.createSocket()).thenReturn(socket);

    when(socketFactory.createPacket(bytesCaptor.capture(), anyInt(),
        addressCaptor.capture())).thenCallRealMethod();
  }

  @Test
  public void connectsToGraphite() throws Exception {
    statsD.connect();

    verify(socketFactory).createSocket();
  }

  @Test
  public void measuresFailures() throws Exception {
    assertThat(statsD.getFailures()).isZero();
  }

  @Test
  public void disconnectsFromGraphite() throws Exception {
    statsD.connect();
    statsD.close();

    verify(socket).close();
  }

  @Test
  public void doesNotAllowDoubleConnections() throws Exception {
    statsD.connect();
    try {
      statsD.connect();
      failBecauseExceptionWasNotThrown(IllegalStateException.class);
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).isEqualTo("Already connected");
    }
  }

  @Test
  public void writesValuesToStatsD() throws Exception {
    statsD.connect();
    statsD.send("name", "value");

    assertThat(new String(bytesCaptor.getValue())).isEqualTo("name:value|g");
  }

  @Test
  public void sanitizesNames() throws Exception {
    statsD.connect();
    statsD.send("name woo", "value");

    assertThat(new String(bytesCaptor.getValue())).isEqualTo("name-woo:value|g");
  }

  @Test
  public void sanitizesValues() throws Exception {
    statsD.connect();
    statsD.send("name", "value woo");

    assertThat(new String(bytesCaptor.getValue())).isEqualTo("name:value-woo|g");
  }

  @Test
  public void address() throws IOException {
    statsD.connect();
    statsD.send("name", "value");

    assertThat(addressCaptor.getValue()).isEqualTo(address);
  }
}
