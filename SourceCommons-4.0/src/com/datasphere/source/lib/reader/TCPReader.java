package com.datasphere.source.lib.reader;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.recovery.CheckpointDetail;
import com.datasphere.source.lib.prop.Property;

public class TCPReader extends WASocket
{
    private SocketChannel channel;
    private ServerSocketChannel serverSocket;
    private String serverHostName;
    private int listenPort;
    
    public TCPReader(final Property prop) throws AdapterException {
        super(prop);
    }
    
    @Override
    protected void init() throws AdapterException {
        super.init();
        this.listenPort = this.property.portno;
        this.serverHostName = this.property.ipaddress;
        if (this.listenPort != 0) {
            this.listen();
        }
        else {
            this.connect();
        }
    }
    
    public void listen() throws AdapterException {
        try {
            this.serverSocket = ServerSocketChannel.open();
            InetAddress serverAddr;
            if (this.serverHostName == null || this.serverHostName == "") {
                serverAddr = InetAddress.getLocalHost();
            }
            else {
                serverAddr = InetAddress.getByName(this.serverHostName);
            }
            final InetSocketAddress addr = new InetSocketAddress(serverAddr, this.listenPort);
            this.serverSocket.bind(addr);
            this.serverSocket.configureBlocking(false);
        }
        catch (IOException e) {
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
        }
    }
    
    @Override
    protected void registerWithSelector(final Selector selector) throws AdapterException {
        try {
            this.serverSocket.register(selector, 16);
        }
        catch (ClosedChannelException e) {
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION, (Throwable)e);
        }
    }
    
    @Override
    public Object readBlock() throws AdapterException {
        return this.readBlock(this.channel);
    }
    
    @Override
    public void connect() throws AdapterException {
        System.out.println("Connecting to...");
        try {
            this.clientSocket = new Socket(this.serverIP, this.serverPort);
        }
        catch (UnknownHostException unknownHostExc) {
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION);
        }
        catch (IOException ioExc) {
            throw new AdapterException(Error.INVALID_IP_ADDRESS);
        }
    }
    
    @Override
    public void close() throws IOException {
        if (this.clientSocket != null) {
            this.clientSocket.close();
        }
        if (this.serverSocket != null) {
            this.serverSocket.close();
        }
    }
    
    @Override
    protected Object readBlock(final SelectableChannel channel) throws AdapterException {
        final SocketChannel sockChannel = (SocketChannel)channel;
        final ByteBuffer buff = ByteBuffer.allocate(this.blockSize());
        try {
            final int bytesRead = sockChannel.read(buff);
            if (bytesRead == -1) {
                sockChannel.close();
                return null;
            }
            buff.limit(bytesRead);
            buff.flip();
        }
        catch (IOException exp) {
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION);
        }
        return buff;
    }
    
    @Override
    public CheckpointDetail getCheckpointDetail() {
        return this.recoveryCheckpoint;
    }
}
