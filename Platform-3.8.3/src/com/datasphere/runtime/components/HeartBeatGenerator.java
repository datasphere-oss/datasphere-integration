package com.datasphere.runtime.components;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.datasphere.event.SimpleEvent;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.channels.Channel;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.TaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.uuid.UUID;

public class HeartBeatGenerator extends IStreamGenerator
{
    private static Logger logger;
    private volatile boolean running;
    private volatile ScheduledFuture<?> task;
    private Channel output;
    private long interval;
    private long heartbeats;
    private int batchSize;
    private int limit;
    private long nextval;
    private long maxnextval;
    
    public HeartBeatGenerator(final MetaInfo.StreamGenerator info, final BaseServer srv) {
        super(srv, info);
        this.running = false;
        this.heartbeats = 0L;
        this.batchSize = 1;
        this.limit = Integer.MAX_VALUE;
        this.nextval = 0L;
        this.maxnextval = 0L;
        this.output = srv.createChannel(this);
        this.interval = ((Number)info.args[0]).longValue();
        if (info.args.length > 1) {
            this.limit = ((Number)info.args[1]).intValue();
            if (info.args.length > 2) {
                this.batchSize = ((Number)info.args[2]).intValue();
                if (info.args.length > 3) {
                    this.maxnextval = ((Number)info.args[3]).longValue();
                }
            }
        }
    }
    
    void publish(final long timestamp) throws Exception {
        final List<DARecord> batch = new ArrayList<DARecord>(this.batchSize);
        for (int i = 0; i < this.batchSize; ++i) {
            final HeartBeatEvent ev = new HeartBeatEvent(timestamp);
            if (this.nextval == this.maxnextval) {
                this.nextval = 0L;
            }
            ev.value = this.nextval++;
            ev.setPayload(new Object[] { ev.value });
            batch.add(new DARecord((Object)ev));
        }
        final TaskEvent ev2 = TaskEvent.createStreamEvent(batch);
        this.output.publish((ITaskEvent)ev2);
        ++this.heartbeats;
    }
    
    @Override
    public void run() {
        try {
            if (!this.running) {
                return;
            }
            if (this.heartbeats == this.limit) {
                return;
            }
            long lastTimestamp = System.nanoTime() / 1000L;
            this.publish(lastTimestamp);
            while (this.running) {
                if (this.heartbeats == this.limit) {
                    return;
                }
                final long now = System.nanoTime() / 1000L;
                if (lastTimestamp + this.interval > now) {
                    this.schedule();
                    return;
                }
                this.publish(lastTimestamp);
                lastTimestamp = now;
            }
        }
        catch (RejectedExecutionException e2) {
            HeartBeatGenerator.logger.warn((Object)"generator terminated due to shutdown");
        }
        catch (InterruptedException ex) {}
        catch (Throwable e) {
            HeartBeatGenerator.logger.error((Object)e);
        }
    }
    
    @Override
    public void close() throws Exception {
        this.stop();
        this.output.close();
    }
    
    private void schedule() {
        final ScheduledExecutorService service = this.srv().getScheduler();
        this.task = service.schedule(this, this.interval, TimeUnit.MICROSECONDS);
    }
    
    @Override
    public Channel getChannel() {
        return this.output;
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
    }
    
    @Override
    public void start() throws Exception {
        if (this.running) {
            return;
        }
        this.running = true;
        this.schedule();
    }
    
    @Override
    public void stop() throws Exception {
        if (!this.running) {
            return;
        }
        this.running = false;
        if (this.task != null) {
            this.task.cancel(true);
            this.task = null;
        }
    }
    
    @Override
    public boolean isRunning() {
        return this.running;
    }
    
    static {
        HeartBeatGenerator.logger = Logger.getLogger((Class)HeartBeatGenerator.class);
    }
    
    public static class HeartBeatEvent extends SimpleEvent implements Serializable
    {
        private static final long serialVersionUID = 7990852358854948290L;
        public static UUID uuid;
        public long value;
        
        public HeartBeatEvent(final long timestamp) {
            super(timestamp);
        }
        
        public String toString() {
            return "heartbeat(" + this.timeStamp + "," + this.value + ")";
        }
        
        static {
            HeartBeatEvent.uuid = new UUID("1288474f-c991-455d-8de3-c2e4d08ed1c0");
        }
    }
}
