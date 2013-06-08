package com.readytalk.metrics;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public interface UdpSocketProvider {
  DatagramSocket newSocket() throws SocketException;

  DatagramPacket newPacket(byte[] buffer) throws IOException;
}