package com.datasphere.source.lib.socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.source.lib.constant.Constant;
import com.datasphere.source.lib.intf.Notify;
import com.datasphere.source.lib.prop.Property;

public abstract class SocketReader
{
    static Logger logger;
    private SocketChannel socketChannel;
    public String remoteAddress;
    public int serverPort;
    protected ByteBuffer byteBuffer;
    protected byte[] leftOverByteBuffer;
    long seekPosition;
    protected long connectionTimeout;
    protected int readTimeout;
    protected long startTime;
    int blockSize;
    Notify rs;
    Selector selector;
    Selector readSelector;
    public char ROW_DELIMITER;
    public char COL_DELIMITER;
    long bytesWritten;
    private boolean connected;
    private int connectionRetry;
    
    public SocketReader(final Notify rs, final Property prop) {
        this.socketChannel = null;
        this.seekPosition = 0L;
        this.connectionTimeout = 0L;
        this.readTimeout = 0;
        this.startTime = 0L;
        this.blockSize = 0;
        this.ROW_DELIMITER = '\0';
        this.COL_DELIMITER = '\0';
        this.bytesWritten = 0L;
        this.connected = false;
        this.connectionRetry = 0;
        this.rs = rs;
        this.loadProperties(prop);
        this.byteBuffer = ByteBuffer.allocateDirect(2 * this.blockSize);
        this.leftOverByteBuffer = new byte[this.blockSize];
    }
    
    private void initializeSocketChannel() throws IOException {
        (this.socketChannel = SocketChannel.open()).configureBlocking(false);
        this.connected = this.socketChannel.connect(new InetSocketAddress(this.remoteAddress, this.serverPort));
        this.startTime = System.currentTimeMillis();
    }
    
    public void connect() throws AdapterException {
        try {
            long connectionAttempt = 0L;
            this.initializeSocketChannel();
            ++connectionAttempt;
            while (!this.connected) {
                try {
                    while (System.currentTimeMillis() - this.startTime < this.connectionTimeout) {
                        if (this.socketChannel.isConnected()) {
                            this.connected = true;
                            this.socketChannel.finishConnect();
                            break;
                        }
                        Thread.sleep(50L);
                    }
                    if (!(this.connected = this.socketChannel.finishConnect())) {
                        final AdapterException se = new AdapterException("Unable to connect host at " + this.remoteAddress + " at port no " + this.serverPort + ". Attempting retry");
                        throw se;
                    }
                    continue;
                }
                catch (AdapterException se) {
                    if (connectionAttempt <= this.connectionRetry || this.connectionRetry < 0) {
                        this.initializeSocketChannel();
                        ++connectionAttempt;
                        continue;
                    }
                    final AdapterException e = new AdapterException(Error.CONNECTION_RETRY_EXCEEDED);
                    throw e;
                }
                catch (InterruptedException e3) {
                    final AdapterException ie = new AdapterException(Error.GENERIC_INTERRUPT_EXCEPTION);
                    throw ie;
                }
            }
            this.selector = Selector.open();
            this.readSelector = Selector.open();
            this.socketChannel.register(this.selector, 5);
            this.socketChannel.register(this.readSelector, 1);
        }
        catch (IOException e2) {
            final AdapterException se2 = new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e2);
            throw se2;
        }
    }
    
    private void loadProperties(final Property prop) {
        this.blockSize = prop.blocksize * 1024;
        this.remoteAddress = prop.remoteaddress;
        this.serverPort = prop.portno;
        this.connectionTimeout = prop.connectionTimeout;
        this.readTimeout = prop.readTimeout;
        this.connectionRetry = prop.retryAttempt;
    }
    
    public void closeSocketReader() throws AdapterException {
        try {
            if (this.socketChannel != null) {
                this.socketChannel.close();
                this.socketChannel = null;
            }
            this.setCurrentSeekPosition(0L);
            if (this.rs != null) {
                this.rs.handleEvent(Constant.eventType.ON_CLOSE, null);
            }
        }
        catch (IOException e) {
            final AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION);
            SocketReader.logger.error((Object)se.getMessage());
            throw se;
        }
    }
    
    public int getBlockSize() {
        return this.blockSize;
    }
    
    public Selector getSelector() {
        return this.selector;
    }
    
    public long getCurrentSeekPosition() {
        return this.seekPosition;
    }
    
    public void setCurrentSeekPosition(final long seekOffset) {
        this.seekPosition = seekOffset;
    }
    
    public char[] getLeftOverBuffer() {
        return null;
    }
    
    public long readBlock() throws AdapterException {
        long readLen = -1L;
        this.getByteBuffer().clear();
        try {
            while (this.readSelector.select(this.readTimeout) > 0) {
                final Set<SelectionKey> readyKeys = this.readSelector.selectedKeys();
                final Iterator<SelectionKey> readyItor = readyKeys.iterator();
                while (readyItor.hasNext()) {
                    final SelectionKey key = readyItor.next();
                    readyItor.remove();
                    final ReadableByteChannel keyChannel = (SocketChannel)key.channel();
                    if (key.isReadable()) {
                        readLen = keyChannel.read(this.byteBuffer);
                        if (readLen <= 0L) {
                            this.resetBuffer();
                            return readLen;
                        }
                        if (readLen > 0L) {
                            break;
                        }
                        continue;
                    }
                }
                if (readLen > 0L) {
                    break;
                }
            }
        }
        catch (IOException e) {
            final AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
            SocketReader.logger.error((Object)se.getMessage());
            throw se;
        }
        this.setCurrentSeekPosition(this.getCurrentSeekPosition() + readLen);
        this.byteBuffer.flip();
        return readLen;
    }
    
    public void send(final ByteBuffer dataToBeSent) throws AdapterException {
        try {
            if (this.getSelector().select(this.readTimeout) > 0) {
                final Set<SelectionKey> readyKeys = this.getSelector().selectedKeys();
                final Iterator<SelectionKey> readyItor = readyKeys.iterator();
                if (readyItor.hasNext()) {
                    final SelectionKey key = readyItor.next();
                    readyItor.remove();
                    final WritableByteChannel writeKeyChannel = (SocketChannel)key.channel();
                    if (key.isWritable()) {
                        while (dataToBeSent.hasRemaining()) {
                            this.bytesWritten += writeKeyChannel.write(dataToBeSent);
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            final AdapterException se = new AdapterException(Error.HOST_CONNECTION_DROPPED, (Throwable)e);
            throw se;
        }
    }
    
    public long getBytesSent() {
        return this.bytesWritten;
    }
    
    private void resetBuffer() {
        this.getByteBuffer().position(0);
        this.getByteBuffer().limit(0);
    }
    
    public String getRemoteAddress() {
        return this.remoteAddress;
    }
    
    public void setRemoteAddress(final String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }
    
    public int getServerPort() {
        return this.serverPort;
    }
    
    public void setServerPort(final int serverPort) {
        this.serverPort = serverPort;
    }
    
    public byte[] getLeftOverByteBuffer() {
        return this.leftOverByteBuffer;
    }
    
    public void close() throws AdapterException {
        this.closeSocketReader();
    }
    
    public ByteBuffer getByteBuffer() {
        return this.byteBuffer;
    }
    
    public SocketChannel getSocketChannel() {
        return this.socketChannel;
    }
    
    static {
        SocketReader.logger = Logger.getLogger((Class)SocketReader.class);
    }
}
