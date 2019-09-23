package com.datasphere.runtime.channels;

import org.apache.log4j.*;
import com.datasphere.runtime.containers.*;
import com.datasphere.exception.*;
import com.datasphere.runtime.components.*;
import java.util.concurrent.*;
import zmq.*;
import java.nio.channels.*;
import org.zeromq.*;

import java.io.*;
import com.datasphere.runtime.monitor.*;

public class BroadcastAsyncChannel extends BroadcastChannel implements Runnable
{
    private static Logger logger;
    private final BlockingQueue<ITaskEvent> eventQueue;
    private volatile boolean running;
    private Future<?> task;
    Long prevReceived;
    Long prevProcessed;
    
    public BroadcastAsyncChannel(final FlowComponent owner) {
        super(owner);
        this.eventQueue = new LinkedBlockingQueue<ITaskEvent>(100);
        this.prevReceived = null;
        this.prevProcessed = null;
        this.running = true;
        final ExecutorService pool = this.srv().getThreadPool();
        this.task = pool.submit(this);
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
        if (this.running) {
            this.setProcessThread();
            this.eventQueue.put(event);
            ++this.received;
        }
    }
    
    @Override
    public void run() {
        while (this.running) {
            try {
                final ITaskEvent event = this.eventQueue.poll(100L, TimeUnit.MILLISECONDS);
                if (event == null) {
                    continue;
                }
                this.doPublish(event);
                continue;
            }
            catch (RejectedExecutionException e5) {
                BroadcastAsyncChannel.logger.info((Object)(this.getOwnerMetaName() + " channel interrupted"));
            }
            catch (InterruptedException e6) {
                BroadcastAsyncChannel.logger.info((Object)(this.getOwnerMetaName() + " channel interrupted"));
            }
            catch (ZError.IOException | ClosedSelectorException | ZMQException ex2) {
                BroadcastAsyncChannel.logger.debug((Object)("Got ZMQException " + ex2.getMessage()));
                continue;
            }
            catch (RuntimeInterruptedException e2) {
                BroadcastAsyncChannel.logger.warn((Object)("Got RuntimeInterruptedException " + e2.getMessage()));
            }
            catch (com.hazelcast.core.RuntimeInterruptedException e3) {
                BroadcastAsyncChannel.logger.warn((Object)("Got RuntimeInterruptedException " + e3.getMessage()));
            }
            catch (Exception e4) {
                BroadcastAsyncChannel.logger.error((Object)("Problem running async broadcast channel " + this.getOwnerMetaName() + ": " + e4.getMessage()));
                continue;
            }
            break;
        }
    }
    
    @Override
    public void close() throws IOException {
        this.resetProcessThread();
        this.running = false;
        this.task.cancel(true);
        this.eventQueue.clear();
        super.close();
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
        final long r = this.received;
        final long p = this.processed;
        final long timeStamp = monEvs.getTimeStamp();
        if (this.prevReceived != r) {
            monEvs.add(MonitorEvent.Type.RECEIVED, Long.valueOf(r));
            if (this.prevReceived != null && this.prevProcessed != null && this.prevTimeStamp != null) {
                monEvs.add(MonitorEvent.Type.RATE, Long.valueOf(1000L * (r - this.prevReceived) / (timeStamp - this.prevTimeStamp)));
                monEvs.add(MonitorEvent.Type.RECEIVED_RATE, Long.valueOf(1000L * (r - this.prevReceived) / (timeStamp - this.prevTimeStamp)));
            }
        }
        if (this.prevProcessed != p) {
            monEvs.add(MonitorEvent.Type.PROCESSED, Long.valueOf(p));
            if (this.prevReceived != null && this.prevProcessed != null && this.prevTimeStamp != null) {
                monEvs.add(MonitorEvent.Type.PROCESSED_RATE, Long.valueOf((p - this.prevProcessed) / (timeStamp - this.prevTimeStamp)));
            }
        }
        this.prevReceived = r;
        this.prevProcessed = p;
    }
    
    static {
        BroadcastAsyncChannel.logger = Logger.getLogger((Class)BroadcastAsyncChannel.class);
    }
}
