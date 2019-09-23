package com.datasphere.runtime.window;

import org.apache.log4j.*;
import com.datasphere.runtime.channels.*;
import com.datasphere.runtime.components.*;
import java.util.*;
import com.datasphere.runtime.containers.*;
import com.datasphere.runtime.*;
import java.io.*;
import java.util.concurrent.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.runtime.monitor.*;

public class ScalaWindow extends IWindow implements Runnable
{
    private static Logger logger;
    private final MetaInfo.Window windowInfo;
    private final Channel output;
    private volatile boolean running;
    private final ServerWrapper srv;
    private Publisher dataSource;
    private final HTimerPolicy timerPolicy;
    private final HBufferFactory windowBufferFactory;
    private final HBufferPolicy bufferPolicy;
    final int count;
    final long interval;
    final CmpAttrs attrComparator;
    
    ScalaWindow(final MetaInfo.Window windowInfo, final ServerWrapper srv, final HTimerPolicy tp, final HBufferFactory wf, final HBufferPolicy bp, final int count, final long interval, final CmpAttrs comparator) throws Exception {
        super(srv.getServer(), windowInfo);
        this.windowInfo = windowInfo;
        (this.output = srv.createChannel(this)).addCallback(this);
        this.srv = srv;
        this.timerPolicy = tp;
        this.windowBufferFactory = wf;
        this.bufferPolicy = bp;
        this.count = count;
        this.interval = interval;
        this.attrComparator = comparator;
    }
    
    @Override
    public Channel getChannel() {
        return this.output;
    }
    
    @Override
    public void close() throws Exception {
        this.stop();
        this.output.close();
        this.srv.destroyKeyFactory(this.windowInfo);
    }
    
    @Override
    public void start() throws Exception {
        if (this.running) {
            return;
        }
        this.running = true;
        this.bufferPolicy.initBuffer(this);
        this.timerPolicy.startTimer(this);
        this.srv.subscribe(this.dataSource, this);
    }
    
    @Override
    public void stop() throws Exception {
        if (!this.running) {
            return;
        }
        this.running = false;
        this.srv.unsubscribe(this.dataSource, this);
        this.timerPolicy.stopTimer(this);
    }
    
    @Override
    public boolean isRunning() {
        return this.running;
    }
    
    @Override
    public void connectParts(final Flow flow) throws Exception {
        this.dataSource = flow.getPublisher(this.windowInfo.stream);
    }
    
    @Override
    public synchronized void receive(final Object linkID, final ITaskEvent event) throws Exception {
        this.bufferPolicy.updateBuffer(this, (Batch)event.batch(), System.nanoTime());
    }
    
    @Override
    public synchronized void run() {
        try {
            this.timerPolicy.onTimer(this);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void notifyMe(final Link link) {
        final Range range = this.bufferPolicy.createSnapshot();
        final TaskEvent ws = TaskEvent.createWindowStateEvent(range);
        try {
            link.subscriber.receive(link.linkID, (ITaskEvent)ws);
        }
        catch (Exception e) {
            ScalaWindow.logger.error((Object)e);
        }
    }
    
    final void publish(final Batch added, final List<DARecord> removed, final Range snapshot) throws Exception {
        this.output.publish((ITaskEvent)TaskEvent.createWindowEvent((IBatch)added, (IBatch)Batch.asBatch(removed), (IRange)snapshot));
    }
    
    final void updateBuffer(final HBuffer w, final Batch batch, final long now) throws Exception {
        w.update(batch, now);
        this.timerPolicy.updateWakeupQueue(this, w, now);
    }
    
    final HBuffer createBuffer(final RecordKey key) {
        final HBuffer b = this.windowBufferFactory.create();
        b.setOwner(this, key);
        return b;
    }
    
    final ScheduledExecutorService getScheduler() {
        return this.srv.getScheduler();
    }
    
    final void jumpingWindowOnTimerCallback() throws Exception {
        this.bufferPolicy.onJumpingTimer();
    }
    
    public static ServerWrapper makeServerWrapper(final Server srv) {
        return new ServerWrapper() {
            @Override
            public Channel createChannel(final FlowComponent owner) {
                return srv.createChannel(owner);
            }
            
            @Override
            public void destroyKeyFactory(final MetaInfo.Window windowInfo) throws Exception {
                KeyFactory.removeKeyFactory(windowInfo, srv);
            }
            
            @Override
            public void subscribe(final Publisher pub, final Subscriber sub) throws Exception {
                srv.subscribe(pub, sub);
            }
            
            @Override
            public void unsubscribe(final Publisher pub, final Subscriber sub) throws Exception {
                srv.unsubscribe(pub, sub);
            }
            
            @Override
            public ScheduledExecutorService getScheduler() {
                return srv.getScheduler();
            }
            
            @Override
            public Server getServer() {
                return srv;
            }
        };
    }
    
    public static ServerWrapper makeTestServerWrapper(final Subscriber sub, final ScheduledExecutorService executor) {
        final Channel output = new Channel() {
            @Override
            public void close() throws IOException {
            }
            
            @Override
            public Collection<MonitorEvent> getMonitorEvents(final long ts) {
                return null;
            }
            
            @Override
            public void publish(final ITaskEvent event) throws Exception {
                sub.receive(null, event);
            }
            
            @Override
            public void addSubscriber(final Link link) {
            }
            
            @Override
            public void removeSubscriber(final Link link) {
            }
            
            @Override
            public void addCallback(final NewSubscriberAddedCallback callback) {
            }
            
            @Override
            public int getSubscribersCount() {
                return 0;
            }
        };
        return new ServerWrapper() {
            @Override
            public Channel createChannel(final FlowComponent owner) {
                return output;
            }
            
            @Override
            public void destroyKeyFactory(final MetaInfo.Window windowInfo) {
            }
            
            @Override
            public void subscribe(final Publisher pub, final Subscriber sub) {
            }
            
            @Override
            public void unsubscribe(final Publisher pub, final Subscriber sub) {
            }
            
            @Override
            public ScheduledExecutorService getScheduler() {
                return executor;
            }
            
            @Override
            public Server getServer() {
                return null;
            }
        };
    }
    
    public static IWindow createWindow(final MetaInfo.Window w, final Server srv) throws Exception {
        final IntervalPolicy l = w.windowLen;
        final boolean jumping = w.jumping;
        final boolean attrBased = l.isAttrBased();
        final boolean countBased = l.isCountBased();
        final boolean timeBased = l.isTimeBased();
        final int count = countBased ? l.getCountPolicy().getCountInterval() : 0;
        final long interval = timeBased ? TimeUnit.MICROSECONDS.toNanos(l.getTimePolicy().getTimeInterval()) : 0L;
        final CmpAttrs attrComparator = attrBased ? AttrExtractor.createAttrComparator(w, srv) : null;
        final KeyFactory keyFactory = w.partitioningFields.isEmpty() ? null : KeyFactory.createKeyFactory(w, srv);
        final ServerWrapper serverWrapper = makeServerWrapper(srv);
        return createWindow(w, jumping, attrBased, countBased, timeBased, count, interval, attrComparator, keyFactory, serverWrapper);
    }
    
    public static IWindow createWindow(final MetaInfo.Window w, final boolean jumping, final boolean attrBased, final boolean countBased, final boolean timeBased, final int count, final long interval_nano, final CmpAttrs attrComparator, final KeyFactory keyFactory, final ServerWrapper serverWrapper) throws Exception {
        HBufferFactory bufferFactory;
        if (attrBased) {
            if (jumping) {
                bufferFactory = new HBufferFactory() {
                    @Override
                    public HBuffer create() {
                        return new HBufferJumpingAttr();
                    }
                };
            }
            else {
                bufferFactory = new HBufferFactory() {
                    @Override
                    public HBuffer create() {
                        return new HBufferSlidingAttr();
                    }
                };
            }
        }
        else if (countBased && !timeBased) {
            if (jumping) {
                bufferFactory = new HBufferFactory() {
                    @Override
                    public HBuffer create() {
                        return new HBufferJumpingCount();
                    }
                };
            }
            else {
                bufferFactory = new HBufferFactory() {
                    @Override
                    public HBuffer create() {
                        return new HBufferSlidingCount();
                    }
                };
            }
        }
        else if (!countBased && timeBased) {
            if (jumping) {
                bufferFactory = new HBufferFactory() {
                    @Override
                    public HBuffer create() {
                        return new HBufferJumpingTime();
                    }
                };
            }
            else {
                bufferFactory = new HBufferFactory() {
                    @Override
                    public HBuffer create() {
                        return new HBufferSlidingTime();
                    }
                };
            }
        }
        else {
            if (!countBased || !timeBased) {
                return null;
            }
            if (jumping) {
                bufferFactory = new HBufferFactory() {
                    @Override
                    public HBuffer create() {
                        return new HBufferJumpingTimeCount();
                    }
                };
            }
            else {
                bufferFactory = new HBufferFactory() {
                    @Override
                    public HBuffer create() {
                        return new HBufferSlidingTimeCount();
                    }
                };
            }
        }
        HTimerPolicy timerPolicy;
        if (timeBased) {
            if (jumping) {
                timerPolicy = new JumpingTimerPolicy();
            }
            else {
                timerPolicy = new SlidingTimerPolicy();
            }
        }
        else {
            timerPolicy = new NoTimerPolicy();
        }
        HBufferPolicy bufferPolicy;
        if (keyFactory != null) {
            if (!jumping && countBased && !timeBased && count == 1) {
                bufferPolicy = new PartitionedBufferPolicy(keyFactory);
            }
            else {
                bufferPolicy = new PartitionedBufferPolicy(keyFactory);
            }
        }
        else {
            bufferPolicy = new SimpleBufferPolicy();
        }
        return new ScalaWindow(w, serverWrapper, timerPolicy, bufferFactory, bufferPolicy, count, interval_nano, attrComparator);
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection events) {
    }
    
    static {
        ScalaWindow.logger = Logger.getLogger((Class)Window.class);
    }
}
