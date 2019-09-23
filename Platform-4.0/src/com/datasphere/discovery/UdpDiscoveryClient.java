package com.datasphere.discovery;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.security.*;
import java.io.*;
import java.net.*;
import java.util.*;
/*
 * 通过 UDP 发现客户端
 */
public class UdpDiscoveryClient
{
    public static final String DISCOVERY_REQUEST_HEADER = "DISCOVER HD SERVER";
    private static final int MAX_DATAGRAM_PACKET_SIZE = 1024;
    private static final int TIMEOUT_IN_MSEC = 5000;
    private static final Logger logger;
    private final int serverDiscoveryPort;
    private final UUID saltId;
    private final String clusterName;
    
    public UdpDiscoveryClient(final int port, final String cluster, final UUID salt) {
        this.serverDiscoveryPort = port;
        this.saltId = salt;
        this.clusterName = cluster;
    }
    
    public String discoverHDServers() {
        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket();
            socket.setBroadcast(true);
            final byte[] request = this.getDiscoveryMessage().getBytes("UTF-8");
            this.sendRequestToBroadcastAddress(socket, request);
            this.sendRequestToAllInterfaces(socket, request);
            final byte[] recvBuf = new byte[1024];
            final DatagramPacket recvPacket = new DatagramPacket(recvBuf, recvBuf.length);
            socket.setSoTimeout(5000);
            int maxAttempts = 10;
            while (maxAttempts-- > 0) {
                socket.receive(recvPacket);
                final String resp = new String(recvPacket.getData()).trim();
                String payload = null;
                if (this.saltId != null) {
                    payload = HSecurityManager.decrypt(resp, this.saltId.toEightBytes());
                }
                else {
                    payload = resp;
                }
                final StringTokenizer tokenizer = new StringTokenizer(payload, "\n");
                if (tokenizer.countTokens() > 1) {
                    final String header = tokenizer.nextToken().trim();
                    if ("HD SERVER NODE".equals(header)) {
                        return tokenizer.nextToken().trim();
                    }
                    continue;
                }
            }
            return null;
        }
        catch (SocketTimeoutException ste) {
            UdpDiscoveryClient.logger.info((Object)"No HD Server could be discovered within the specified timeout. Either specify Server Address or check the network setting to enable broadcast");
            return null;
        }
        catch (Exception e) {
            UdpDiscoveryClient.logger.warn((Object)"No HD Server could be discovered due to exception", (Throwable)e);
            return null;
        }
        finally {
            if (socket != null) {
                socket.close();
            }
        }
    }
    
    private void sendRequestToBroadcastAddress(final DatagramSocket socket, final byte[] request) throws IOException {
        final DatagramPacket packet = new DatagramPacket(request, request.length, InetAddress.getByName("255.255.255.255"), this.serverDiscoveryPort);
        socket.send(packet);
    }
    
    private void sendRequestToAllInterfaces(final DatagramSocket socket, final byte[] request) throws IOException {
        final Enumeration<NetworkInterface> intefaces = NetworkInterface.getNetworkInterfaces();
        while (intefaces.hasMoreElements()) {
            final NetworkInterface networkInterface = intefaces.nextElement();
            if (!networkInterface.isLoopback()) {
                if (!networkInterface.isUp()) {
                    continue;
                }
                for (final InterfaceAddress iAddress : networkInterface.getInterfaceAddresses()) {
                    final InetAddress broadcastAddress = iAddress.getBroadcast();
                    if (broadcastAddress == null) {
                        continue;
                    }
                    final DatagramPacket sendpacket = new DatagramPacket(request, request.length, broadcastAddress, this.serverDiscoveryPort);
                    socket.send(sendpacket);
                }
            }
        }
    }
    
    private String getDiscoveryMessage() {
        final StringBuffer buffer = new StringBuffer("DISCOVER HD SERVER");
        buffer.append("\nCluster:" + this.clusterName);
        buffer.append("\nRequestTime:" + System.currentTimeMillis());
        return buffer.toString();
    }
    
    static {
        logger = Logger.getLogger((Class)UdpDiscoveryClient.class);
    }
}
