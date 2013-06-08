package com.readytalk.metrics;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

@Slf4j
public class DefaultUdpSocketProvider implements UdpSocketProvider {

  private final String host;
  private final int port;

  public DefaultUdpSocketProvider(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public DatagramSocket newSocket() throws SocketException {
    return new DatagramSocket();
  }

  @Override
  public DatagramPacket newPacket(final byte[] buffer) throws IOException {
    try {
      InetAddress address = InetAddress.getByName(host);
      return new DatagramPacket(buffer, buffer.length, address, this.port);
    } catch (UnknownHostException e) {
      if (log.isDebugEnabled()) {
        log.debug(e.getMessage());
      } else {
        log.error("Unable to resolve host", e);
      }
      return null;
    }

  }
}
