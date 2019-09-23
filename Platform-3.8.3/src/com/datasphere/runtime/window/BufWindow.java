package com.datasphere.runtime.window;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.datasphere.recovery.PathManager;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.containers.Batch;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.uuid.UUID;

public abstract class BufWindow implements Runnable
{
    private final ConcurrentSkipListMap<Long, Bucket> elements;
    private transient volatile long vHead;
    private transient volatile long vTail;
    private transient volatile long vOldestSnapshotHead;
    private final SnapshotRefIndex activeSnapshots;
    private final BufferManager owner;
    private RecordKey partKey;
    private PathManager mustExceedThis;
    public static final int BUCKET_SIZE = 64;
    public static final long BUCKET_INDEX_MASK = 63L;
    public static final long BUCKET_MASK = -64L;
    private Bucket newvalues;
    Snapshot snapshot;
    volatile PathManager recentEmittedEvents;
    final SlidingPolicy.OutputBuffer outputBuffer;
    
    protected BufWindow(final BufferManager owner) {
        this.elements = new ConcurrentSkipListMap<Long, Bucket>();
        this.vHead = 0L;
        this.vTail = 0L;
        this.vOldestSnapshotHead = 0L;
        this.recentEmittedEvents = null;
        this.owner = owner;
        this.activeSnapshots = BufferManager.snIndexFac.create();
        this.snapshot = this.makeEmptySnapshot();
        this.outputBuffer = this.getFactory().createOutputBuffer(this);
    }
    
    public BufWindowFactory getFactory() {
        return this.owner.getFactory();
    }
    
    public WindowPolicy getPolicy() {
        return this.getFactory().getPolicy();
    }
    
    public void setPartKey(final RecordKey pk) {
        this.partKey = pk;
    }
    
    public RecordKey getPartKey() {
        return this.partKey;
    }
    
    public int getLogicalSize() {
        return (int)(this.getTail() - this.getHead());
    }
    
    public int numOfElements() {
        return this.elements.size();
    }
    
    public int numOfActiveSnapshots() {
        return this.activeSnapshots.size();
    }
    
    boolean isEmpty() {
        return this.elements.isEmpty();
    }
    
    boolean hasNoActiveSnapshots() {
        return this.activeSnapshots.isEmpty();
    }
    
    boolean canBeShutDown() {
        return this.isEmpty() && this.hasNoActiveSnapshots();
    }
    
    void shutdown() {
        this.cancel();
    }
    
    Snapshot getSnapshot() {
        return this.snapshot;
    }
    
    void addNewRows(final IBatch newEntries) {
        final long timestamp = System.nanoTime();
        if (!this.doAccept(newEntries)) {
            return;
        }
        for (final Object o : newEntries) {
            final DARecord data = (DARecord)o;
            final long id = this.vTail;
            final HEntry e = new HEntry(data, id, timestamp);
            final int idx = (int)(id & 0x3FL);
            if (idx == 0 || this.elements.isEmpty()) {
                this.newvalues = new Bucket(new HEntry[64], id);
                this.elements.put(id & 0xFFFFFFFFFFFFFFC0L, this.newvalues);
            }
            this.newvalues.set(idx, e);
            ++this.vTail;
        }
        this.update(newEntries);
    }
    
    public void removeRefAndSetOldestSnapshot(final SnapshotRef ref) {
        long newOldestSnapshot = this.activeSnapshots.removeAndGetOldest(ref);
        if (newOldestSnapshot == -1L) {
            newOldestSnapshot = this.snapshot.vHead;
        }
        this.clean(newOldestSnapshot);
    }
    
    private void checkInvariants() {
        final long wHead = this.snapshot.vHead;
        final long tail = this.getTail();
        assert this.getHead() <= wHead;
        assert wHead <= tail;
        assert this.getHead() <= this.vOldestSnapshotHead;
        assert this.vOldestSnapshotHead <= tail;
        assert this.vOldestSnapshotHead <= wHead;
    }
    
    private HEntry get(final long index) {
        assert index >= this.getHead();
        assert index < this.getTail();
        final Bucket bucket = this.elements.get(index & 0xFFFFFFFFFFFFFFC0L);
        if (bucket == null) {
            return null;
        }
        return bucket.get((int)(index & 0x3FL));
    }
    
    private List<Bucket> getRange(final long from, final long to) {
        assert from <= to;
        assert from >= this.getHead();
        assert to <= this.getTail();
        final long from_bucket = from & 0xFFFFFFFFFFFFFFC0L;
        final long to_bucket = to - 1L & 0xFFFFFFFFFFFFFFC0L;
        final int nbuckets = (int)((to_bucket - from_bucket) / 64L) + 1;
        if (nbuckets == 1) {
            return Collections.singletonList(this.elements.get(from_bucket));
        }
        long index = from_bucket;
        final List<Bucket> range = new ArrayList<Bucket>(nbuckets);
        for (int i = 0; i < nbuckets; ++i) {
            final Bucket bucket = this.elements.get(index);
            range.add(bucket);
            index += 64L;
        }
        return range;
    }
    
    private void clean(final long oldestSeen) {
        assert this.vOldestSnapshotHead <= oldestSeen;
        assert oldestSeen <= this.getTail();
        if (oldestSeen != this.vOldestSnapshotHead) {
            this.vOldestSnapshotHead = oldestSeen;
            if (oldestSeen == this.getTail()) {
                this.elements.clear();
                this.vHead = this.getTail();
                this.owner.addEmptyBuffer(this);
            }
            else {
                final long head = this.getHead();
                assert oldestSeen >= head;
                if (oldestSeen > head) {
                    final ConcurrentNavigableMap<Long, Bucket> t = this.elements.headMap(Long.valueOf(oldestSeen & 0xFFFFFFFFFFFFFFC0L));
                    t.clear();
                    this.vHead = oldestSeen;
                }
            }
        }
    }
    
    DARecord getData(final long index) {
        if (index == this.getTail()) {
            return null;
        }
        final HEntry e = this.get(index);
        return e.data;
    }
    
    private boolean isSmallSnapshot(final int size) {
        return size <= 64;
    }
    
    Snapshot makeEmptySnapshot() {
        return new EmptySnapshot(this.getTail());
    }
    
    Snapshot makeSnapshot(final Snapshot prev, final int size) {
        final long end = this.getTail();
        final long start = end - size;
        if (start < prev.vHead) {
            return this.makeOverlappingSnapshot(prev, end);
        }
        return this.makeSnapshot(start, end);
    }
    
    Snapshot makeSnapshot(final long start, final long end) {
        assert start <= end;
        assert this.vOldestSnapshotHead <= this.getTail();
        assert start >= this.vOldestSnapshotHead;
        assert end <= this.getTail();
        final int size = (int)(end - start);
        if (size == 0) {
            return new EmptySnapshot(start);
        }
        if (size == 1) {
            return new OneItemSnapshot(this.get(start));
        }
        if (this.isSmallSnapshot(size)) {
            return new BuffersListSnapshot(start, end, this.getRange(start, end));
        }
        if (this.owner.isInlineCleanup()) {
            return new BuffersListSnapshot(start, end, this.getRange(start, end));
        }
        return this.makeRangeSnapshot(start, end);
    }
    
    Snapshot makeOneItemSnapshot(final long start) {
        assert this.vOldestSnapshotHead <= this.getTail();
        assert start >= this.vOldestSnapshotHead;
        assert start <= this.getTail();
        return new OneItemSnapshot(this.get(start));
    }
    
    private Snapshot makeOverlappingSnapshot(final Snapshot sn, final long end) {
        final int size = (int)(end - sn.vHead);
        if (size == sn.size()) {
            return sn;
        }
        assert sn.vHead <= end;
        assert end <= this.getTail();
        if (size == 1) {
            return new OneItemSnapshot(this.get(sn.vHead));
        }
        if (this.isSmallSnapshot(size)) {
            return new BuffersListSnapshot(sn.vHead, end, this.getRange(sn.vHead, end));
        }
        if (sn instanceof MapRangeSnapshot) {
            if (this.owner.isInlineCleanup()) {
                return new BuffersListSnapshot(sn.vHead, end, this.getRange(sn.vHead, end));
            }
            return new OverlappingSnapshot((MapRangeSnapshot)sn, end, this.elements);
        }
        else {
            if (this.owner.isInlineCleanup()) {
                return new BuffersListSnapshot(sn.vHead, end, this.getRange(sn.vHead, end));
            }
            return this.makeRangeSnapshot(sn.vHead, end);
        }
    }
    
    private Snapshot makeRangeSnapshot(final long start, final long end) {
        final Snapshot sn = new MapRangeSnapshot(start, end, this.elements);
        if (!this.owner.isInlineCleanup()) {
            this.activeSnapshots.add(this, sn, this.owner.getRefQueue());
        }
        return sn;
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        final long wHead = this.snapshot.vHead;
        sb.append(" PartKey:").append(this.partKey);
        sb.append(" Head:").append(this.getHead());
        sb.append(" Tail:").append(this.getTail());
        sb.append(" Window:").append(wHead);
        sb.append(" OldestSnapshot:").append(this.vOldestSnapshotHead);
        sb.append("\n");
        if (this.elements.isEmpty()) {
            sb.append("No data\n");
        }
        else {
            String sep = "";
            sb.append("[\n");
            for (final Bucket vals : this.elements.values()) {
                for (int len = vals.length(), j = 0; j < len; ++j) {
                    final HEntry e = vals.get(j);
                    if (e != null) {
                        if (e.id < this.getHead()) {
                            continue;
                        }
                        if (e.id >= this.getTail()) {
                            continue;
                        }
                    }
                    if (e != null && e.id == wHead) {
                        sb.append("|\n");
                    }
                    else {
                        sb.append(sep);
                    }
                    sb.append(e);
                    sep = ",\n";
                }
            }
            sb.append("\n]\n");
        }
        sb.append("Window(").append(this.getFactory()).append(" snaphot:").append(this.snapshot).append(" at ").append(this.snapshot.vHead).append(")");
        return sb.toString();
    }
    
    void checkAndDumpState(final PrintStream out) {
        this.checkInvariants();
        out.println(this);
    }
    
    void dumpActiveSnapshots(final PrintStream out) {
        final List<? extends SnapshotRef> l = this.activeSnapshots.toList();
        final int sz = l.size();
        out.println("dump active snapshots: count:" + sz + " partKey:" + this.partKey);
        final int n = (sz <= 100) ? sz : 50;
        final int i = 0;
        for (final SnapshotRef ref : l) {
            if (i == n) {
                break;
            }
            out.println(ref);
        }
        if (sz <= 100) {
            return;
        }
        out.println("...");
        final Iterator<? extends SnapshotRef> it = l.listIterator(l.size() - 50);
        while (it.hasNext()) {
            final SnapshotRef ref = (SnapshotRef)it.next();
            out.println(ref);
        }
    }
    
    void dumpBuffer(final PrintStream out) {
        final int sz = (int)(this.getTail() - this.getHead());
        out.println("dump buffer: elemnts count:" + sz + " partKey:" + this.partKey);
        final int n = (sz <= 100) ? sz : 50;
        int i = 0;
        for (final Bucket vals : this.elements.values()) {
            for (int len = vals.length(), j = 0; j < len; ++j) {
                final HEntry e = vals.get(j);
                if (e != null && e.id >= this.getHead()) {
                    if (e.id < this.getTail()) {
                        if (i++ == n) {
                            break;
                        }
                        out.println(e);
                    }
                }
            }
        }
        if (sz <= 100) {
            return;
        }
        out.println("...");
        i = 0;
        final Iterator<Long> it = this.elements.keySet().descendingIterator();
    Label_0338:
        while (it.hasNext()) {
            final Bucket vals = this.elements.get(it.next());
            for (int len = vals.length(), j = 0; j < len; ++j) {
                final HEntry e = vals.get(j);
                if (e != null && e.id >= this.getHead()) {
                    if (e.id < this.getTail()) {
                        if (i++ == 50) {
                            break Label_0338;
                        }
                        out.println(e);
                    }
                }
            }
        }
    }
    
    List<DARecord> setNewWindowSnapshot(final Snapshot newsn) {
        final long oldHead = this.snapshot.vHead;
        final long newHead = newsn.vHead;
        if (oldHead == newHead) {
            this.snapshot = newsn;
            return Collections.emptyList();
        }
        assert newHead > oldHead;
        final Snapshot diff = this.makeOverlappingSnapshot(this.snapshot, newHead);
        this.snapshot = newsn;
        if (this.activeSnapshots.isEmpty()) {
            this.clean(this.snapshot.vHead);
        }
        return diff;
    }
    
    ScheduledExecutorService getScheduler() {
        return this.owner.getScheduler();
    }
    
    long getTail() {
        return this.vTail;
    }
    
    long getHead() {
        return this.vHead;
    }
    
    boolean doAccept(final IBatch newEntries) {
        final UUID windowid = this.owner.getWindowID();
        final PathManager waitPosition = this.owner.windowCheckpoints.get(windowid);
        if (waitPosition == null) {
            return true;
        }
        final String distId = (this.partKey != null) ? this.partKey.toString() : null;
        if (this.mustExceedThis == null) {
            if (distId != null) {
                this.mustExceedThis = waitPosition.getLowPositionForComponent(windowid, distId);
            }
            else {
                this.mustExceedThis = waitPosition.getLowPositionForComponent(windowid);
            }
        }
        for (final Object o : newEntries) {
            final Position dataPositionAugmented = ((DARecord)o).position.createAugmentedPosition(windowid, distId);
            if (!this.mustExceedThis.blocks(dataPositionAugmented)) {
                this.mustExceedThis.removePathsWhichStartWith(dataPositionAugmented.values());
                return true;
            }
        }
        return false;
    }
    
    protected abstract void update(final IBatch p0);
    
    protected abstract void flush();
    
    void cancel() {
        if (this.outputBuffer != null) {
            this.outputBuffer.stop();
        }
    }
    
    private void publish(final IBatch newEntries, final List<DARecord> oldEntries) {
        if (this.outputBuffer != null) {
            this.outputBuffer.update(this.snapshot, newEntries, (IBatch)Batch.asBatch(oldEntries));
        }
        else {
            this.getFactory().receive(this.partKey, this.snapshot, newEntries, (IBatch)Batch.asBatch(oldEntries));
        }
        if (this.owner.isInlineCleanup()) {
            this.clean(this.snapshot.vHead);
        }
    }
    
    void notifyOnUpdate(final IBatch newEntries, final List<DARecord> oldEntries) {
        this.publish(newEntries, oldEntries);
        for (final Object o : newEntries) {
            final DARecord ev = (DARecord)o;
            if (this.recentEmittedEvents == null) {
                synchronized (this) {
                    if (this.recentEmittedEvents == null) {
                        this.recentEmittedEvents = new PathManager();
                    }
                }
            }
            this.recentEmittedEvents.mergeLowerPositions(ev.position);
        }
    }
    
    void notifyOnTimer(final IBatch newEntries, final List<DARecord> oldEntries) {
        this.publish(newEntries, oldEntries);
    }
    
    void notifyOnUpdate(final List<DARecord> newEntries, final List<DARecord> oldEntries) {
        this.notifyOnUpdate((IBatch)Batch.asBatch(newEntries), oldEntries);
    }
    
    void notifyOnTimer(final List<DARecord> newEntries, final List<DARecord> oldEntries) {
        this.notifyOnTimer((IBatch)Batch.asBatch(newEntries), oldEntries);
    }
    
    Snapshot makeNewAttrSnapshot(final IBatch newEntries, final CmpAttrs attrComparator) {
        long newHead;
        final long tail = newHead = this.getTail();
        final DARecord last = (DARecord)newEntries.last();
        final Iterator<HEntry> it = this.snapshot.itemIterator();
        while (it.hasNext()) {
            final HEntry e = it.next();
            if (attrComparator.inRange(e.data, last)) {
                newHead = e.id;
                break;
            }
        }
        if (newHead == tail) {
            int n = newEntries.size();
            for (final Object o : newEntries) {
                final DARecord obj = (DARecord)o;
                if (attrComparator.inRange(obj, last)) {
                    newHead = tail - n;
                    break;
                }
                --n;
            }
        }
        final Snapshot newsn = this.makeSnapshot(newHead, tail);
        return newsn;
    }
    
    protected Future<?> schedule(final long interval) {
        return this.getScheduler().schedule(this, interval, TimeUnit.NANOSECONDS);
    }
    
    protected Future<?> scheduleAtFixedRate(final long interval) {
        return this.getScheduler().scheduleAtFixedRate(this, interval, interval, TimeUnit.NANOSECONDS);
    }
    
    @Override
    public void run() {
        try {
            this.onTimer();
        }
        catch (Throwable e) {
            e.printStackTrace(System.err);
            throw e;
        }
    }
    
    protected void onTimer() {
        throw new UnsupportedOperationException();
    }
    
    static class Bucket extends AtomicReferenceArray<HEntry>
    {
        private static final long serialVersionUID = -5690701562483619810L;
        final long id;
        
        Bucket(final HEntry[] vals, final long id) {
            super(vals);
            this.id = id;
        }
        
        @Override
        public String toString() {
            final char[] s = new char[this.length()];
            for (int i = 0; i < this.length(); ++i) {
                s[i] = ((this.get(i) == null) ? '_' : '#');
            }
            return this.id + " [" + new String(s) + "]";
        }
    }
}
