package com.datasphere.source.lib.reader;

import org.apache.log4j.*;

import com.datasphere.common.exc.*;
import com.datasphere.recovery.*;

import java.io.*;
import java.nio.channels.*;
import java.util.*;

public class ThreadedQueueReader extends QueueReader implements Runnable
{
    private Selector selector;
    private Thread selectThread;
    private boolean doneWithThread;
    Logger logger;
    
    public ThreadedQueueReader(final ReaderBase link) throws AdapterException {
        super(link);
        this.logger = Logger.getLogger((Class)ThreadedQueueReader.class);
        this.isThreaded = true;
    }
    
    @Override
    protected void init() throws AdapterException {
        super.init();
        this.doneWithThread = false;
        (this.selectThread = new Thread(this)).start();
    }
    
    @Override
    public void run() {
        try {
            this.selector = Selector.open();
            this.linkedStrategy.registerWithSelector(this.selector);
            while (!this.doneWithThread) {
                this.selector.select();
                final Iterator<SelectionKey> itr = this.selector.selectedKeys().iterator();
                while (itr.hasNext()) {
                    final SelectionKey event = itr.next();
                    itr.remove();
                    if (event.isAcceptable()) {
                        final ServerSocketChannel serverChannel = (ServerSocketChannel)event.channel();
                        final SocketChannel clientSocket = serverChannel.accept();
                        clientSocket.configureBlocking(false);
                        clientSocket.register(this.selector, 1);
                    }
                    else {
                        if (!event.isReadable()) {
                            continue;
                        }
                        final Object buffer = this.linkedStrategy.readBlock(event.channel());
                        if (buffer != null) {
                            final Map<String, Object> meta = this.linkedStrategy.getEventMetadata();
                            this.enqueue(buffer, meta);
                        }
                        else {
                            event.channel().close();
                        }
                    }
                }
            }
            this.linkedStrategy.close();
        }
        catch (AdapterException soExp) {
            this.logger.error((Object)soExp.getErrorMessage());
        }
        catch (IOException e) {
            this.logger.error((Object)e.getMessage());
        }
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        if (this.selectThread != null && !this.doneWithThread) {
            this.doneWithThread = true;
            while (this.selectThread.getState() != Thread.State.TERMINATED) {
                try {
                    this.selector.wakeup();
                    Thread.sleep(100L);
                }
                catch (InterruptedException e) {
                    this.logger.warn((Object)"Threaded close interrupted");
                }
            }
            this.closeClientConnection();
            this.selector.close();
        }
        else if (this.doneWithThread && this.logger.isDebugEnabled()) {
            this.logger.debug((Object)"close() is called on already closed adapter");
        }
    }
    
    private void closeClientConnection() throws IOException {
        final Iterator<SelectionKey> itr = this.selector.keys().iterator();
        SelectionKey event = null;
        while (itr.hasNext()) {
            event = itr.next();
            event.channel().close();
        }
    }
    
    @Override
    public CheckpointDetail getCheckpointDetail() {
        return this.recoveryCheckpoint;
    }
}
