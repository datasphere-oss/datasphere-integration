package com.datasphere.runtime.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.BlockingQueue;

import org.jctools.queues.MpscCompoundQueue;


public class NetLogger
{
    private AsynchronousSocketChannel socket;
    private final InetSocketAddress hostAddress;
    private final BlockingQueue<ByteBuffer> queue;
    private volatile boolean closed;
    
    public static PrintStream out() {
        return SingletonHolder.INSTANCE;
    }
    
    public static PrintStream create(final int port) {
        return new NetLogger(port).createPrintStream();
    }
    
    private NetLogger(final int port) {
        this.closed = false;
        this.queue = new MpscCompoundQueue<ByteBuffer>(1024);
        try {
            this.hostAddress = new InetSocketAddress(InetAddress.getByName("localhost"), port);
            this.socket = AsynchronousSocketChannel.open();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.reconnect(ByteBuffer.allocate(0));
    }
    
    private void reconnect(final ByteBuffer buf) {
        if (this.closed) {
            return;
        }
        if (!this.socket.isOpen()) {
            try {
                this.socket = AsynchronousSocketChannel.open();
            }
            catch (IOException e1) {
                e1.printStackTrace();
                return;
            }
        }
        this.socket.connect(this.hostAddress, buf, new CompletionHandler<Void, ByteBuffer>() {
            @Override
            public void completed(final Void result, final ByteBuffer buf) {
                NetLogger.this.socket.write(buf, buf, new CompletionHandler<Integer, ByteBuffer>() {
                    @Override
                    public void completed(final Integer result, ByteBuffer buf) {
                        if (result == -1) {
                            NetLogger.this.reconnect(buf);
                            return;
                        }
                        if (!buf.hasRemaining()) {
                            try {
                                buf = NetLogger.this.queue.take();
                            }
                            catch (InterruptedException | IllegalArgumentException ex2) {
                                return;
                            }
                        }
                        NetLogger.this.socket.write(buf, buf, this);
                    }
                    
                    @Override
                    public void failed(final Throwable exc, final ByteBuffer buf) {
                        if (exc instanceof IOException) {
                            try {
                                NetLogger.this.socket.close();
                            }
                            catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        NetLogger.this.reconnect(buf);
                    }
                });
            }
            
            @Override
            public void failed(final Throwable e, final ByteBuffer buf) {
                try {
                    Thread.sleep(2000L);
                }
                catch (InterruptedException ex) {}
                NetLogger.this.reconnect(buf);
            }
        });
    }
    
    private PrintStream createPrintStream() {
        return new PrintStream(new OutputStream() {
            final ByteArrayOutputStream pbuf = new ByteArrayOutputStream();
            
            @Override
            public void write(final int b) throws IOException {
                this.pbuf.write(b);
                if (b == 10) {
                    NetLogger.this.queue.offer(ByteBuffer.wrap(this.pbuf.toByteArray()));
                    this.pbuf.reset();
                }
            }
            
            @Override
            public void close() throws IOException {
                NetLogger.this.closed = true;
                NetLogger.this.socket.close();
            }
        });
    }
    
    private static class SingletonHolder
    {
        private static final PrintStream INSTANCE;
        
        static {
            INSTANCE = NetLogger.create(55555);
        }
    }
}
