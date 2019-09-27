package com.datasphere.source.lib.reader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.source.lib.prop.Property;

public class UDPReader extends HDSocket
{
    private String serverName;
    private int serverPort;
    private ByteBuffer buffer;
    private DatagramChannel socketChannel;
    private Logger logger;
    Map<String, Object> metadata;
    
    public UDPReader(final Property prop) throws AdapterException {
        super(prop);
        this.logger = Logger.getLogger((Class)UDPReader.class);
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("UDPReader is initialized with following properties\nIPAddress - [" + prop.ipaddress + "]\nPortNo - [" + prop.portno + "]\nBlockSize - [" + prop.blocksize + "]"));
        }
    }
    
    public void init() throws AdapterException {
        super.init();
        this.loadConfig();
        try {
            final InetSocketAddress sockAddr = new InetSocketAddress(this.serverName, this.serverPort);
            this.socketChannel = DatagramChannel.open();
            this.socketChannel.socket().setReuseAddress(true);
            this.socketChannel.socket().bind(sockAddr);
            this.socketChannel.configureBlocking(false);
            if (this.logger.isTraceEnabled()) {
                this.logger.trace((Object)("UPD Server is initialized using IPAddress " + this.serverName + " , listening at " + this.serverPort));
            }
        }
        catch (SocketException sockExp) {
            throw new AdapterException(Error.GENERIC_EXCEPTION, "Got SocketException", (Throwable)sockExp);
        }
        catch (SecurityException securityExp) {
            throw new AdapterException(Error.GENERIC_EXCEPTION, "Got SecurityException", (Throwable)securityExp);
        }
        catch (IOException ioExp) {
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)ioExp);
        }
    }
    
    private void loadConfig() {
        this.serverName = this.property().ipaddress;
        this.serverPort = this.property().portno;
        this.name("UDP:" + this.serverName + ":" + this.serverPort);
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        return this.readBlock(this.socketChannel);
    }
    
    @Override
    protected Object readBlock(final SelectableChannel channel) throws AdapterException {
        final DatagramChannel udpChannel = (DatagramChannel)channel;
        (this.buffer = ByteBuffer.allocate(this.blockSize())).clear();
        try {
            final SocketAddress remoteAddr = udpChannel.receive(this.buffer);
            final InetSocketAddress sockAddr = (InetSocketAddress)remoteAddr;
            this.eventMetadataMap.put(Reader.SOURCE_IP, sockAddr.getAddress().toString());
            this.eventMetadataMap.put(Reader.SOURCE_PORT, sockAddr.getPort());
        }
        catch (IOException exp) {
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)exp);
        }
        this.buffer.flip();
        if (this.logger.isTraceEnabled()) {
            try {
                this.logger.trace((Object)("UDPReader has read " + this.buffer.limit() + " bytes from " + udpChannel.getRemoteAddress()));
            }
            catch (IOException e) {
                throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
            }
        }
        return this.buffer;
    }
    
    @Override
    public void close() throws IOException {
        if (this.logger.isTraceEnabled()) {
            this.logger.trace((Object)("UDPReader is closed and disconnected from " + this.socketChannel.getRemoteAddress()));
        }
        this.socketChannel.close();
    }
    
    public void connect(final InetSocketAddress addr) throws AdapterException {
        try {
            (this.socketChannel = DatagramChannel.open()).connect(addr);
            if (this.logger.isTraceEnabled()) {
                this.logger.trace((Object)("UDP client is initialized to send datagram to Server with IPAddress " + addr.getHostString() + " listening at " + addr.getPort()));
            }
        }
        catch (UnknownHostException unknowHostExc) {
            throw new AdapterException(Error.INVALID_IP_ADDRESS, (Throwable)unknowHostExc);
        }
        catch (IOException ioExc) {
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)ioExc);
        }
    }
    
    @Override
    protected void registerWithSelector(final Selector selector) throws AdapterException {
        try {
            this.socketChannel.register(selector, 1);
        }
        catch (ClosedChannelException e) {
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
        }
    }
    
    @Override
    public CheckpointDetail getCheckpointDetail() {
        return this.recoveryCheckpoint;
    }
}
