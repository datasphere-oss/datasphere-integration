package com.datasphere.runtime.window;

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.CustomThreadFactory;
import com.datasphere.runtime.KeyFactory;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.Server;
import com.datasphere.runtime.ServerServices;
import com.datasphere.runtime.containers.Batch;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.Range;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.uuid.UUID;

class PartitionedBufferManager extends BufferManager
{
    private final Queue<BufWindow> emptyBuffers;
    private final Future<?> removeEmptyBufsTask;
    private final Lock bufLock;
    private final ConcurrentHashMap<RecordKey, BufWindow> buffers;
    private final KeyFactory keyFactory;
    private final ScheduledThreadPoolExecutor scheduler;
    private volatile int createdPartitions;
    private volatile int removedPartitions;
    long prevSize;
    long prevHead;
    long prevTail;
    long prevNumPartitions;
    long prevIn;
    long prevInRate;
    
    PartitionedBufferManager(final UUID windowid, final BufWindowFactory windowDesc, final KeyFactory keyFactory, final ServerServices srv, final boolean inlineCleanup) {
        super(windowid, windowDesc, srv, inlineCleanup);
        this.emptyBuffers = new ConcurrentLinkedQueue<BufWindow>();
        this.bufLock = new ReentrantLock();
        this.buffers = new ConcurrentHashMap<RecordKey, BufWindow>();
        this.createdPartitions = 0;
        this.removedPartitions = 0;
        this.prevSize = -1L;
        this.prevHead = -1L;
        this.prevTail = -1L;
        this.prevNumPartitions = -1L;
        this.prevIn = -1L;
        this.prevInRate = -1L;
        this.keyFactory = keyFactory;
        final String wname = windowid.getUUIDString();
        this.scheduler = new ScheduledThreadPoolExecutor(1, new CustomThreadFactory("Window_Scheduler_" + wname, true));
        srv.scheduleStatsReporting(Server.statsReporter(wname, this.scheduler), 5, true);
        this.removeEmptyBufsTask = srv.getScheduler().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                PartitionedBufferManager.this.removeEmptyBuffers();
            }
        }, 2L, 2L, TimeUnit.SECONDS);
    }
    
    @Override
    public void shutdown() {
        super.shutdown();
        this.removeEmptyBufsTask.cancel(true);
        this.scheduler.shutdown();
        try {
            if (!this.scheduler.awaitTermination(5L, TimeUnit.SECONDS)) {
                this.scheduler.shutdownNow();
            }
        }
        catch (InterruptedException ie) {
            this.scheduler.shutdownNow();
        }
    }
    
    private void removeEmptyBuffers() {
        while (!this.emptyBuffers.isEmpty()) {
            final BufWindow buffer = this.emptyBuffers.poll();
            if (buffer == null) {
                break;
            }
            if (!buffer.canBeShutDown()) {
                continue;
            }
            if (this.bufLock.tryLock()) {
                try {
                    if (!buffer.canBeShutDown()) {
                        continue;
                    }
                    buffer.shutdown();
                    this.buffers.remove(buffer.getPartKey(), buffer);
                    ++this.removedPartitions;
                }
                finally {
                    this.bufLock.unlock();
                }
            }
            else {
                this.emptyBuffers.offer(buffer);
            }
        }
    }
    
    private Collection<BufWindow> getBuffers() {
        return this.buffers.values();
    }
    
    private void addNewRows(final RecordKey key, final IBatch keybatch) {
        this.bufLock.lock();
        try {
            BufWindow b = this.buffers.get(key);
            if (b == null) {
                if (PartitionedBufferManager.logger.isDebugEnabled()) {
                    PartitionedBufferManager.logger.debug((Object)("Creating new window partition for key " + key));
                }
                b = this.createBuffer(key);
                this.buffers.put(key, b);
                ++this.createdPartitions;
            }
            b.addNewRows(keybatch);
        }
        finally {
            this.bufLock.unlock();
        }
    }
    
    @Override
    void closeBuffers() {
        for (final BufWindow b : this.getBuffers()) {
            b.shutdown();
        }
    }
    
    @Override
    public void receiveImpl(final Object linkID, final ITaskEvent event) throws Exception {
        final IBatch batch = event.batch();
        for (final Object o : Batch.partition(this.keyFactory, (IBatch<DARecord>)batch).entrySet()) {
            final Map.Entry<RecordKey, IBatch> e = (Map.Entry<RecordKey, IBatch>)o;
            final RecordKey key = e.getKey();
            final IBatch keybatch = e.getValue();
            this.addNewRows(key, keybatch);
        }
    }
    
    @Override
    void dumpBuffer(final PrintStream out) {
        for (final BufWindow b : this.getBuffers()) {
            b.dumpBuffer(out);
        }
    }
    
    @Override
    public void dumpActiveSnapshots(final PrintStream out) {
        for (final BufWindow b : this.getBuffers()) {
            b.dumpActiveSnapshots(out);
        }
    }
    
    @Override
    public Range makeSnapshot() {
        final Map<RecordKey, IBatch> map = new HashMap<RecordKey, IBatch>();
        for (final BufWindow b : this.getBuffers()) {
            final Snapshot sn = b.getSnapshot();
            map.put(b.getPartKey(), (IBatch)Batch.asBatch(sn));
        }
        return (Range)Range.createRange(map);
    }
    
    @Override
    public void getCheckpoint(final Window window, final PathManager checkpoint) {
        for (final BufWindow buffer : this.getBuffers()) {
            final Snapshot sn = buffer.getSnapshot();
            if (sn != null) {
                final String distId = buffer.getPartKey().toPartitionKey();
                for (final DARecord ev : sn) {
                    if (ev.position != null) {
                        final Position augmentedPosition = ev.position.createAugmentedPosition(window.getMetaID(), distId);
                        checkpoint.mergeLowerPositions(augmentedPosition);
                    }
                    else {
                        if (!PartitionedBufferManager.logger.isDebugEnabled()) {
                            continue;
                        }
                        PartitionedBufferManager.logger.debug((Object)"Unexpected request for current window position but found null event position");
                    }
                }
            }
        }
    }
    
    @Override
    public void memoryStatus(final Window window) {
        Logger.getLogger("Command").warn((Object)("---------- MEMORY STATUS for WINDOW " + window.getMetaName() + " ----------"));
        for (final BufWindow buffer : this.getBuffers()) {
            final Snapshot sn = buffer.getSnapshot();
            if (sn != null) {
                final String distId = buffer.getPartKey().toPartitionKey();
                int counter = 1;
                for (final DARecord ev : sn) {
                    Logger.getLogger("Command").warn((Object)("-- Event" + counter++ + ": " + ev));
                }
            }
        }
    }
    
    @Override
    public void flushAll() {
        final Map<RecordKey, BufWindow> newbufs = new HashMap<RecordKey, BufWindow>();
        for (final BufWindow buffer : this.getBuffers()) {
            buffer.shutdown();
            final RecordKey pk = buffer.getPartKey();
            final BufWindow b = this.createBuffer(pk);
            newbufs.put(pk, b);
        }
        this.bufLock.lock();
        try {
            this.buffers.clear();
            this.buffers.putAll(newbufs);
        }
        finally {
            this.bufLock.unlock();
        }
    }
    
    @Override
    public void flushAllForQuiesce() {
        final Map<RecordKey, BufWindow> newbufs = new HashMap<RecordKey, BufWindow>();
        for (final BufWindow buffer : this.getBuffers()) {
            buffer.flush();
        }
    }
    
    @Override
    public void addSpecificMonitorEventsForWindow(final Window window, final MonitorEventsCollection monEvs) {
        final Collection<BufWindow> bufs = this.getBuffers();
        if (bufs.isEmpty()) {
            return;
        }
        long size = 0L;
        long head = Long.MIN_VALUE;
        long tail = Long.MAX_VALUE;
        final long in = this.getInputCount();
        for (final BufWindow b : bufs) {
            final Snapshot s = b.getSnapshot();
            if (s != null) {
                size += s.size();
                head = ((head < s.vHead) ? s.vHead : head);
                if (!(s instanceof RangeSnapshot)) {
                    continue;
                }
                final RangeSnapshot r = (RangeSnapshot)s;
                tail = ((tail > r.vTail) ? r.vTail : tail);
            }
        }
        final long numPartitions = bufs.size();
        boolean showedActivity = false;
        if (this.prevSize != size) {
            monEvs.add(MonitorEvent.Type.WINDOW_SIZE, Long.valueOf(size));
            this.prevSize = size;
            showedActivity = true;
        }
        if (this.prevNumPartitions != numPartitions) {
            monEvs.add(MonitorEvent.Type.NUM_PARTITIONS, Long.valueOf(numPartitions));
            this.prevNumPartitions = numPartitions;
            showedActivity = true;
        }
        if (this.prevHead != head) {
            monEvs.add(MonitorEvent.Type.RANGE_HEAD, Long.valueOf(head));
            this.prevHead = head;
            showedActivity = true;
        }
        if (this.prevTail != tail && tail != Long.MAX_VALUE) {
            monEvs.add(MonitorEvent.Type.RANGE_TAIL, Long.valueOf(tail));
            this.prevTail = tail;
            showedActivity = true;
        }
        if (this.prevIn != in) {
            monEvs.add(MonitorEvent.Type.INPUT, Long.valueOf(in));
            final Long inRate = monEvs.getRate(in, this.prevIn);
            if (inRate != null) {
                monEvs.add(MonitorEvent.Type.INPUT_RATE, inRate);
                this.prevInRate = inRate;
            }
            this.prevIn = in;
            showedActivity = true;
        }
        if (showedActivity) {
            monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
        }
    }
    
    @Override
    void addEmptyBuffer(final BufWindow buffer) {
        this.emptyBuffers.offer(buffer);
    }
    
    @Override
    public Stats getStats() {
        final PStats s = new PStats();
        s.received = this.getInputCount();
        s.numOfPartitions = this.buffers.size();
        for (final BufWindow w : this.getBuffers()) {
            if (w != null) {
                if (w.snapshot.isEmpty()) {
                    final PStats pStats = s;
                    ++pStats.numOfEmptyPartitions;
                }
                else {
                    final PStats pStats2 = s;
                    ++pStats2.numOfNonEmptyPartitions;
                }
            }
            final PStats pStats3 = s;
            pStats3.logicalSize += w.getLogicalSize();
            final PStats pStats4 = s;
            pStats4.numOfActiveSnapshots += w.numOfActiveSnapshots();
            final PStats pStats5 = s;
            pStats5.size += w.numOfElements();
        }
        s.createdPartitions = this.createdPartitions;
        s.removedPartitions = this.removedPartitions;
        s.numOfEmptyBufs = this.emptyBuffers.size();
        return s;
    }
    
    @Override
    ScheduledExecutorService getScheduler() {
        return this.scheduler;
    }
    
    private static class PStats extends Stats
    {
        int received;
        int numOfPartitions;
        int numOfActiveSnapshots;
        int size;
        int logicalSize;
        int createdPartitions;
        int removedPartitions;
        int numOfEmptyPartitions;
        int numOfNonEmptyPartitions;
        int numOfEmptyBufs;
        
        private PStats() {
            this.received = 0;
            this.numOfPartitions = 0;
            this.numOfActiveSnapshots = 0;
            this.size = 0;
            this.logicalSize = 0;
            this.createdPartitions = 0;
            this.removedPartitions = 0;
            this.numOfEmptyPartitions = 0;
            this.numOfNonEmptyPartitions = 0;
            this.numOfEmptyBufs = 0;
        }
        
        @Override
        public String toString() {
            return "received:" + this.received + "\npartitions: " + this.numOfPartitions + "(" + this.createdPartitions + "-" + this.removedPartitions + ") snapshots: " + this.numOfActiveSnapshots + " treeElems: " + this.size + " size: " + this.logicalSize + "\nempty parts:" + this.numOfEmptyPartitions + " non-empty parts:" + this.numOfNonEmptyPartitions + " empty buffers:" + this.numOfEmptyBufs;
        }
    }
}
