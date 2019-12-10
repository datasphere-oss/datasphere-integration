package com.datasphere.runtime.window;

import org.apache.log4j.*;
import com.datasphere.runtime.meta.*;
import java.io.*;
import java.util.concurrent.*;
import com.datasphere.runtime.channels.*;
import com.datasphere.runtime.monitor.*;
import com.datasphere.proc.events.commands.*;
import com.datasphere.runtime.components.*;
import com.datasphere.runtime.utils.*;
import com.datasphere.recovery.*;
import org.joda.time.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.containers.*;
import java.util.*;

public class Window extends IWindow implements BufWindowSub
{
    private static Logger logger;
    private final MetaInfo.Window windowInfo;
    private final BroadcastAsyncChannel output;
    private BufWindowFactory wndDesc;
    private Stream dataSource;
    private volatile boolean running;
    private volatile boolean closed;
    private BufferManager bufMgr;
    private Future<?> statsTask;
    private volatile PrintStream tracer;
    private final boolean inlineCleanup;
    private final Queue<LagMarker> lagMarkerQueue;
    
    public Window(final MetaInfo.Window windowInfo, final BaseServer srv) throws Exception {
        super(srv, windowInfo);
        this.running = false;
        this.closed = false;
        this.lagMarkerQueue = new ConcurrentLinkedQueue<LagMarker>();
        this.windowInfo = windowInfo;
        (this.output = srv.createChannel(this)).addCallback(this);
        final String val = System.getProperty("com.dss.window.inlinecleanup");
        if (val != null && !val.isEmpty()) {
            if (val.equalsIgnoreCase("false")) {
                this.inlineCleanup = false;
            }
            else {
                this.inlineCleanup = true;
            }
        }
        else {
            this.inlineCleanup = true;
        }
    }
    
    @Override
    public void connectParts(final Flow flow) throws Exception {
        this.dataSource = (Stream)flow.getPublisher(this.windowInfo.stream);
        this.wndDesc = BufWindowFactory.create(this.windowInfo, this.srv(), this);
        this.bufMgr = BufferManager.create(this, this.wndDesc, this.inlineCleanup);
    }
    
    @Override
    public Channel getChannel() {
        return this.output;
    }
    
    @Override
    public void close() throws Exception {
        if (this.closed) {
            return;
        }
        this.closed = true;
        this.stop();
        if (this.dataSource != null) {
            if (this.wndDesc != null && this.wndDesc.getPolicy().getComparator() != null) {
                AttrExtractor.removeAttrExtractor(this.windowInfo);
            }
            this.bufMgr.shutdown();
        }
        this.output.close();
        KeyFactory.removeKeyFactory(this.windowInfo, this.srv());
    }
    
    @Override
    public void addSpecificMonitorEvents(final MonitorEventsCollection monEvs) {
        if (this.bufMgr != null) {
            this.bufMgr.addSpecificMonitorEventsForWindow(this, monEvs);
        }
    }
    
    @Override
    public void notifyMe(final Link link) {
        final Range range = this.bufMgr.makeSnapshot();
        final TaskEvent ws = TaskEvent.createWindowStateEvent(range);
        try {
            link.subscriber.receive(link.linkID, (ITaskEvent)ws);
        }
        catch (Exception e) {
            Window.logger.error((Object)e);
        }
    }
    
    @Override
    public void receive(final Object linkID, final ITaskEvent event) throws Exception {
        if (event instanceof CommandEvent) {
            ((CommandEvent)event).performCommand(this);
            return;
        }
        if (this.shouldMarkerBePassedAlong(event)) {
            this.lagMarkerQueue.offer(((TaskEvent)event).getLagMarker().copy());
        }
        this.bufMgr.receive(linkID, event);
    }
    
    @Override
    public void flush() throws Exception {
        this.bufMgr.flushAllForQuiesce();
    }
    
    @Override
    public void publish(final ITaskEvent event) throws Exception {
        this.output.publish(event);
    }
    
    @Override
    public void start() throws Exception {
        if (this.running) {
            return;
        }
        this.srv().subscribe(this.dataSource, this);
        this.running = true;
        if (this.windowInfo.options != null) {
            final int port = ((Number)this.windowInfo.options).intValue();
            this.tracer = ((port == 0) ? null : NetLogger.create(port));
            this.statsTask = this.srv().scheduleStatsReporting(new Runnable() {
                @Override
                public void run() {
                    Window.this.dumpStats();
                }
            }, 2, false);
        }
    }
    
    @Override
    public void stop() throws Exception {
        if (!this.running) {
            return;
        }
        this.srv().unsubscribe(this.dataSource, this);
        this.bufMgr.flushAll();
        this.running = false;
        if (this.statsTask != null) {
            this.statsTask.cancel(true);
            if (this.tracer != null) {
                this.tracer.close();
                this.tracer = null;
            }
        }
    }
    
    @Override
    public boolean isRunning() {
        return this.running;
    }
    
    @Override
    public Position getCheckpoint() {
        final PathManager result = new PathManager();
        this.bufMgr.getCheckpoint(this, result);
        if (Window.logger.isTraceEnabled()) {
            Window.logger.trace((Object)("Returning window " + this.getMetaName() + " current position: " + result));
        }
        return result.toPosition();
    }
    
    public void setWaitPosition(final Position position) {
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
            Logger.getLogger("Recovery").debug((Object)("Setting window " + this.getMetaName() + " wait position to " + position));
        }
        this.bufMgr.setWaitPosition(position);
    }
    
    public BufWindowFactory getWindowDesc() {
        return this.wndDesc;
    }
    
    public MetaInfo.Window getWindowMeta() {
        return this.windowInfo;
    }
    
    public void dumpStats() {
        final PrintStream out = (this.tracer == null) ? NetLogger.out() : this.tracer;
        try {
            final BufferManager.Stats s = this.bufMgr.getStats();
            out.println(DateTime.now().toLocalTime() + " ------- window:" + this.getMetaName());
            out.println(s);
        }
        catch (Throwable e) {
            e.printStackTrace(out);
            throw e;
        }
    }
    
    @Override
    public void receive(final RecordKey partKey, final Collection<DARecord> snapshot, final IBatch added, final IBatch removed) {
        final String distId = (partKey != null) ? partKey.toPartitionKey() : null;
        final List<DARecord> newAdded = new ArrayList<DARecord>(added.size());
        for (final Object o : added) {
            final DARecord DARecord = (DARecord)o;
            if (DARecord.position == null) {
                newAdded.add(DARecord);
            }
            else {
                try {
                    final DARecord newInstance = (DARecord)DARecord.getClass().newInstance();
                    newInstance.initValues(DARecord);
                    newInstance.position = newInstance.position.createAugmentedPosition(this.windowInfo.uuid, distId);
                    newAdded.add(newInstance);
                }
                catch (InstantiationException | IllegalAccessException ex3) {
                    Window.logger.error((Object)ex3);
                }
            }
        }
        final List<DARecord> newRemoved = new ArrayList<DARecord>(removed.size());
        for (final Object o2 : removed) {
            final DARecord DARecord2 = (DARecord)o2;
            if (DARecord2.position == null) {
                newRemoved.add(DARecord2);
            }
            else {
                try {
                    final DARecord newInstance2 = (DARecord)DARecord2.getClass().newInstance();
                    newInstance2.initValues(DARecord2);
                    newInstance2.position = newInstance2.position.createAugmentedPosition(this.windowInfo.uuid, distId);
                    newRemoved.add(newInstance2);
                }
                catch (InstantiationException | IllegalAccessException ex4) {
                    Window.logger.error((Object)ex4);
                }
            }
        }
        final TaskEvent event = TaskEvent.createWindowEvent((IBatch)Batch.asBatch(newAdded), (IBatch)Batch.asBatch(newRemoved), (IRange)Range.createRange(partKey, snapshot));
        try {
            final LagMarker lagMarker = this.lagMarkerQueue.poll();
            if (lagMarker != null) {
                this.recordLagMarker(lagMarker);
                event.setLagRecord(true);
                event.setLagMarker(lagMarker);
                this.lagMarkerQueue.clear();
                this.lagRecordInfo(this.getMetaInfo(), (ITaskEvent)event);
            }
            this.output.publish((ITaskEvent)event);
        }
        catch (InterruptedException e3) {
            Window.logger.debug((Object)("during publishing window event:" + e3));
        }
        catch (Exception e4) {
            throw new RuntimeException(e4);
        }
    }
    
    static {
        Window.logger = Logger.getLogger((Class)Window.class);
    }
}
