package com.datasphere.discovery;

import org.apache.log4j.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;
import com.datasphere.metaRepository.*;
import java.net.*;
import java.nio.channels.*;
import java.util.*;
import com.datasphere.security.*;
import java.security.*;
import java.io.*;
import java.nio.*;

/*
 * 通过 UDP 发现服务器
 */
public class UdpDiscoveryServer implements Runnable
{
    public static final String DISCOVERY_RESPONSE_HEADER = "HD SERVER NODE";
    private static final int MAX_DATAGRAM_PACKET_SIZE = 1024;
    private static final Logger logger;
    private final int boundPort;
    private final String hazelcastBindAddress;
    private final int hazelcastBindPort;
    private final UUID salt;
    private final String clusterName;
    private DatagramChannel channel;
    
    public UdpDiscoveryServer(final int boundPort, final UUID salt) {
        this(boundPort, salt, HazelcastSingleton.getBindingInterface(), ((InetSocketAddress)HazelcastSingleton.get().getLocalEndpoint().getSocketAddress()).getPort(), HazelcastSingleton.getClusterName());
    }
    
    public UdpDiscoveryServer(final int boundPort, final UUID salt, final String serverIP, final int serverPort, final String cluster) {
        this.boundPort = boundPort;
        this.salt = salt;
        this.hazelcastBindAddress = serverIP;
        this.hazelcastBindPort = serverPort;
        this.clusterName = cluster;
    }
    
    @Override
    public void run() {
        try {
            final Selector selector = Selector.open();
            (this.channel = DatagramChannel.open()).configureBlocking(false);
            this.channel.register(selector, 1);
            try {
                this.channel.socket().bind(new InetSocketAddress(this.boundPort));
                this.channel.register(selector, 1, new ClientRecord());
                while (!Thread.currentThread().isInterrupted()) {
                    if (selector.select() == 0) {
                        continue;
                    }
                    final Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator();
                    while (keyIter.hasNext()) {
                        final SelectionKey key = keyIter.next();
                        if (key.isReadable()) {
                            this.handleRead(key);
                        }
                        if (key.isValid() && key.isWritable()) {
                            this.handleWrite(key);
                        }
                        keyIter.remove();
                    }
                }
            }
            finally {
                if (this.channel != null) {
                    this.channel.close();
                    this.channel = null;
                }
            }
        }
        catch (ClosedByInterruptException ex) {}
        catch (Exception e) {
            UdpDiscoveryServer.logger.warn((Object)"UdpDiscoveryServer stopping to respond to Discovery requests", (Throwable)e);
        }
    }
    // 验证发现请求
    private boolean verifyDiscoveryRequest(final String discoveryPayload) throws ClosedByInterruptException {
        final StringTokenizer tokenizer = new StringTokenizer(discoveryPayload, "\n");
        if (tokenizer.countTokens() < 2) {
            return false;
        }
        final String header = tokenizer.nextToken();
        final String requestedCluster = tokenizer.nextToken();
        if (header == null || header.trim().isEmpty()) {
            return false;
        }
        if (!"DISCOVER HD SERVER".equals(header)) {
            return false;
        }
        if (requestedCluster == null || requestedCluster.trim().isEmpty()) {
            return false;
        }
        final StringTokenizer clusterTokenizer = new StringTokenizer(requestedCluster, ":");
        if (clusterTokenizer.countTokens() < 2) {
            return false;
        }
        final String clusterHeader = clusterTokenizer.nextToken();
        final String requestedClusterName = clusterTokenizer.nextToken();
        return this.clusterName.equals(requestedClusterName);
    }
    
    private String formatResponse() throws UnsupportedEncodingException, GeneralSecurityException {
        final StringBuffer buffer = new StringBuffer("HD SERVER NODE \n");
        buffer.append(this.hazelcastBindAddress + ":" + this.hazelcastBindPort);
        if (this.salt != null) {
            return HSecurityManager.encrypt(buffer.toString(), this.salt.toEightBytes());
        }
        return buffer.toString();
    }
    // 处理读操作
    private void handleRead(final SelectionKey key) throws IOException {
        final DatagramChannel channel = (DatagramChannel)key.channel();
        final ClientRecord clientRecord = (ClientRecord)key.attachment();
        clientRecord.buffer.clear();
        clientRecord.clientAddress = channel.receive(clientRecord.buffer);
        if (clientRecord.clientAddress != null && this.verifyDiscoveryRequest(new String(clientRecord.buffer.array()))) {
            key.interestOps(4);
        }
    }
    // 处理写操作
    private void handleWrite(final SelectionKey key) throws GeneralSecurityException, IOException {
        final DatagramChannel channel = (DatagramChannel)key.channel();
        final ClientRecord clientRecord = (ClientRecord)key.attachment();
        clientRecord.buffer.clear();
        clientRecord.buffer.put(this.formatResponse().getBytes("UTF-8"));
        clientRecord.buffer.flip();
        final int bytesSent = channel.send(clientRecord.buffer, clientRecord.clientAddress);
        if (bytesSent != 0) {
            key.interestOps(1);
        }
    }
    
    static {
        logger = Logger.getLogger((Class)UdpDiscoveryServer.class);
    }
    
    private static class ClientRecord
    {
        public SocketAddress clientAddress;
        public ByteBuffer buffer;
        
        private ClientRecord() {
            this.buffer = ByteBuffer.allocate(1024);
        }
    }
}
