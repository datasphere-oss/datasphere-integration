package com.datasphere.runtime.window;

import java.io.PrintStream;
import java.lang.ref.ReferenceQueue;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.apache.log4j.Logger;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.PositionResponse;
import com.datasphere.runtime.BaseServer;
import com.datasphere.runtime.KeyFactory;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.ServerServices;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.Range;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.uuid.UUID;

public abstract class BufferManager implements MessageListener<PositionResponse>
{
    public static boolean removeEmptyPartitions;
    protected static final Logger logger;
    static SnapshotRefIndexFactory snIndexFac;
    private volatile int inputCount;
    private final UUID windowid;
    private final ReferenceQueue<Snapshot> rqueue;
    private Future<?> remover;
    private volatile boolean finished;
    private final BufWindowFactory windowDesc;
    protected Map<UUID, PathManager> windowCheckpoints;
    private String positionRequestMessageListenerRegId;
    Map<BufWindowFactory, PathManager> bufferCheckpoints;
    private final boolean inlineCleanup;
    
    BufferManager(final UUID windowid, final BufWindowFactory windowDesc, final ServerServices srv, final boolean inlineCleanup) {
        this.inputCount = 0;
        this.rqueue = new ReferenceQueue<Snapshot>();
        this.finished = false;
        this.windowCheckpoints = new HashMap<UUID, PathManager>();
        this.positionRequestMessageListenerRegId = null;
        this.bufferCheckpoints = new HashMap<BufWindowFactory, PathManager>();
        this.windowid = windowid;
        this.windowDesc = windowDesc;
        if (!(this.inlineCleanup = inlineCleanup)) {
            this.remover = srv.getThreadPool().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (!BufferManager.this.finished) {
                            try {
                                final SnapshotRef ref = (SnapshotRef)BufferManager.this.rqueue.remove();
                                final BufWindow buffer = ref.buffer;
                                buffer.removeRefAndSetOldestSnapshot(ref);
                                continue;
                            }
                            catch (InterruptedException e2) {}
                            break;
                        }
                    }
                    catch (Throwable e) {
                        e.printStackTrace();
                        throw e;
                    }
                }
            });
        }
        final ITopic<PositionResponse> topic = HazelcastSingleton.get().getTopic("#SharedCheckpoint_" + windowid);
        this.positionRequestMessageListenerRegId = topic.addMessageListener((MessageListener)this);
    }
    
    public UUID getWindowID() {
        return this.windowid;
    }
    
    public BufWindowFactory getFactory() {
        return this.windowDesc;
    }
    
    public boolean isInlineCleanup() {
        return this.inlineCleanup;
    }
    
    public abstract Stats getStats();
    
    public abstract void flushAll();
    
    public abstract void flushAllForQuiesce();
    
    public abstract void memoryStatus(final Window p0);
    
    public abstract void getCheckpoint(final Window p0, final PathManager p1);
    
    public abstract Range makeSnapshot();
    
    public abstract void dumpActiveSnapshots(final PrintStream p0);
    
    public abstract void addSpecificMonitorEventsForWindow(final Window p0, final MonitorEventsCollection p1);
    
    public abstract void receiveImpl(final Object p0, final ITaskEvent p1) throws Exception;
    
    abstract void closeBuffers();
    
    abstract void dumpBuffer(final PrintStream p0);
    
    abstract void addEmptyBuffer(final BufWindow p0);
    
    abstract ScheduledExecutorService getScheduler();
    
    public static BufferManager create(final Window window, final BufWindowFactory windowDesc, final boolean inlineCleanup) {
        final MetaInfo.Window wi = window.getWindowMeta();
        final BaseServer srv = window.srv();
        final UUID windowid = wi.getUuid();
        try {
            if (wi.partitioningFields.isEmpty()) {
                return new NoPartBufferManager(windowid, windowDesc, srv, inlineCleanup);
            }
            final KeyFactory kf = KeyFactory.createKeyFactory(wi, srv);
            return new PartitionedBufferManager(windowid, windowDesc, kf, srv, inlineCleanup);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
    
    public static BufferManager createTestBufferManager(final BufWindowFactory windowDesc, final ScheduledExecutorService scheduler, final ExecutorService es) {
        return new NoPartBufferManager(null, windowDesc, new ServerServices() {
            @Override
            public ScheduledExecutorService getScheduler() {
                return scheduler;
            }
            
            @Override
            public ExecutorService getThreadPool() {
                return es;
            }
            
            @Override
            public ScheduledFuture<?> scheduleStatsReporting(final Runnable task, final int period, final boolean opt) {
                return null;
            }
        }, false);
    }
    
    public static BufferManager createPartitionedTestBufferManager(final BufWindowFactory windowDesc, final ScheduledExecutorService scheduler, final ExecutorService es, final KeyFactory kf) {
        return new PartitionedBufferManager(null, windowDesc, kf, new ServerServices() {
            @Override
            public ScheduledExecutorService getScheduler() {
                return scheduler;
            }
            
            @Override
            public ExecutorService getThreadPool() {
                return es;
            }
            
            @Override
            public ScheduledFuture<?> scheduleStatsReporting(final Runnable task, final int period, final boolean opt) {
                return null;
            }
        }, false);
    }
    
    public void receive(final Object linkID, final ITaskEvent event) throws Exception {
        this.inputCount += event.batch().size();
        this.receiveImpl(linkID, event);
    }
    
    public int getInputCount() {
        return this.inputCount;
    }
    
    public void shutdown() {
        this.finished = true;
        if (this.remover != null) {
            this.remover.cancel(true);
        }
        this.windowDesc.removeAllSubsribers();
        this.closeBuffers();
        final ITopic<PositionResponse> topic = HazelcastSingleton.get().getTopic("#SharedCheckpoint_" + this.windowid);
        topic.removeMessageListener(this.positionRequestMessageListenerRegId);
    }
    
    ReferenceQueue<Snapshot> getRefQueue() {
        return this.rqueue;
    }
    
    @Override
    public String toString() {
        return "BufferManager(" + this.windowDesc + ")";
    }
    
    protected BufWindow createBuffer(final RecordKey partKey) {
        final BufWindow b = this.windowDesc.createWindow(this);
        b.setPartKey(partKey);
        return b;
    }
    
    public final void setWaitPosition(final Position position) {
        this.flushAll();
        this.windowCheckpoints.put(this.windowid, new PathManager(position));
    }
    
    public void onMessage(final Message<PositionResponse> message) {
        final PositionResponse pr = (PositionResponse)message.getMessageObject();
        if (!Server.server.getServerID().equals((Object)pr.fromServer)) {
            final Position dataPositionAugmented = pr.position;
            if (BufferManager.logger.isDebugEnabled()) {
                BufferManager.logger.debug((Object)("BufferManagerImpl received shared-checkpoint notify of removal for " + dataPositionAugmented));
            }
            final PathManager bufferCheckpoint = this.windowCheckpoints.get(pr.fromID);
            if (bufferCheckpoint != null) {
                bufferCheckpoint.removePathsWhichStartWith(dataPositionAugmented.values());
            }
        }
    }
    
    static {
        BufferManager.removeEmptyPartitions = true;
        logger = Logger.getLogger((Class)BufferManager.class);
        BufferManager.snIndexFac = FibonacciHeapSnapshotRefIndex.factory;
    }
    
    public abstract static class Stats
    {
        @Override
        public abstract String toString();
    }
}
