package com.datasphere.runtime.window;

import java.io.PrintStream;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.log4j.Logger;

import com.datasphere.recovery.Path;
import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.recovery.SourcePosition;
import com.datasphere.runtime.ServerServices;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.Range;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.runtime.monitor.MonitorEvent;
import com.datasphere.runtime.monitor.MonitorEventsCollection;
import com.datasphere.uuid.UUID;

class NoPartBufferManager extends BufferManager
{
    private final ServerServices srv;
    private BufWindow buffer;
    private PathManager waitPosition;
    Long prevSize;
    Long prevHead;
    Long prevTail;
    Long prevIn;
    Long prevInRate;
    
    NoPartBufferManager(final UUID windowid, final BufWindowFactory windowDesc, final ServerServices srv, final boolean inlineCleanup) {
        super(windowid, windowDesc, srv, inlineCleanup);
        this.waitPosition = new PathManager();
        this.prevSize = null;
        this.prevHead = null;
        this.prevTail = null;
        this.prevIn = null;
        this.prevInRate = null;
        this.srv = srv;
        this.buffer = this.createBuffer(null);
    }
    
    @Override
    void closeBuffers() {
        this.buffer.shutdown();
    }
    
    @Override
    public void receiveImpl(final Object linkID, final ITaskEvent event) throws Exception {
        final IBatch batch = event.batch();
        this.buffer.addNewRows(batch);
    }
    
    protected boolean isBeforeWaitPosition(final Position position) {
        if (this.waitPosition == null || position == null) {
            return false;
        }
        for (final Path hdPath : position.values()) {
            if (!this.waitPosition.containsKey(hdPath.getPathHash())) {
                continue;
            }
            final SourcePosition sp = this.waitPosition.get(hdPath.getPathHash()).getLowSourcePosition();
            if (hdPath.getLowSourcePosition().compareTo(sp) <= 0) {
                if (NoPartBufferManager.logger.isInfoEnabled()) {
                    NoPartBufferManager.logger.info((Object)("Rejection HD as duplicate -- position=" + position));
                }
                return true;
            }
            this.waitPosition.removePath(hdPath.getPathHash());
        }
        if (this.waitPosition.isEmpty()) {
            this.waitPosition = null;
        }
        return false;
    }
    
    @Override
    void dumpBuffer(final PrintStream out) {
        this.buffer.dumpBuffer(out);
    }
    
    @Override
    public void dumpActiveSnapshots(final PrintStream out) {
        this.buffer.dumpActiveSnapshots(out);
    }
    
    @Override
    public Range makeSnapshot() {
        final Snapshot sn = this.buffer.getSnapshot();
        return Range.createRange(null, sn);
    }
    
    @Override
    public void getCheckpoint(final Window window, final PathManager checkpoint) {
        final Snapshot sn = this.buffer.getSnapshot();
        if (sn != null) {
            for (final DARecord ev : sn) {
                if (ev.position != null) {
                    final Position augmentedPosition = ev.position.createAugmentedPosition(window.getMetaID(), (String)null);
                    checkpoint.mergeLowerPositions(augmentedPosition);
                }
            }
        }
    }
    
    @Override
    public void memoryStatus(final Window window) {
        Logger.getLogger("Command").warn((Object)("---------- MEMORY STATUS for WINDOW " + window.getMetaName() + " ----------"));
        final Snapshot sn = this.buffer.getSnapshot();
        if (sn != null) {
            final String distId = this.buffer.getPartKey().toPartitionKey();
            int counter = 1;
            for (final DARecord ev : sn) {
                Logger.getLogger("Command").warn((Object)("-- Event" + counter++ + ": " + ev));
            }
        }
    }
    
    @Override
    public void flushAll() {
        this.buffer.shutdown();
        this.buffer = this.createBuffer(null);
    }
    
    @Override
    public void flushAllForQuiesce() {
        this.buffer.flush();
    }
    
    @Override
    public void addSpecificMonitorEventsForWindow(final Window window, final MonitorEventsCollection monEvs) {
        if (this.buffer == null) {
            return;
        }
        final long head = this.buffer.getHead();
        final long tail = this.buffer.getTail();
        final long in = this.getInputCount();
        boolean showedActivity = false;
        final Snapshot s = this.buffer.getSnapshot();
        if (s != null) {
            final long size = s.size();
            if (this.prevSize == null || this.prevSize != size) {
                monEvs.add(MonitorEvent.Type.WINDOW_SIZE, Long.valueOf(size));
                this.prevSize = size;
                showedActivity = true;
            }
        }
        if (this.prevHead == null || this.prevHead != head) {
            monEvs.add(MonitorEvent.Type.RANGE_HEAD, Long.valueOf(head));
            this.prevHead = head;
            showedActivity = true;
        }
        if (this.prevTail == null || this.prevTail != tail) {
            monEvs.add(MonitorEvent.Type.RANGE_TAIL, Long.valueOf(tail));
            this.prevTail = tail;
            showedActivity = true;
        }
        if (this.prevIn == null || this.prevIn != in) {
            monEvs.add(MonitorEvent.Type.INPUT, Long.valueOf(in));
            this.prevIn = in;
            showedActivity = true;
        }
        final Long inRate = monEvs.getRate(in, this.prevIn);
        if (inRate != null) {
            monEvs.add(MonitorEvent.Type.INPUT_RATE, inRate);
            monEvs.add(MonitorEvent.Type.RATE, inRate);
        }
        this.prevInRate = inRate;
        if (showedActivity) {
            monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
        }
    }
    
    @Override
    void addEmptyBuffer(final BufWindow buffer) {
    }
    
    @Override
    public Stats getStats() {
        return new Stats() {
            int received = NoPartBufferManager.this.getInputCount();
            int logicalSize = NoPartBufferManager.this.buffer.getLogicalSize();
            int numOfActiveSnapshots = NoPartBufferManager.this.buffer.numOfActiveSnapshots();
            int size = NoPartBufferManager.this.buffer.numOfElements();
            
            @Override
            public String toString() {
                return "received:" + this.received + " snapshots: " + this.numOfActiveSnapshots + " treeElems: " + this.size + " size: " + this.logicalSize;
            }
        };
    }
    
    @Override
    ScheduledExecutorService getScheduler() {
        return this.srv.getScheduler();
    }
}
