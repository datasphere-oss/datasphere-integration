package com.datasphere.runtime.window;

import java.util.concurrent.*;
import java.util.*;
import java.lang.ref.*;

class SkipListSnapshotRefIndex implements SnapshotRefIndex
{
    public static final SnapshotRefIndexFactory factory;
    private static final Comparator<CmpSnapshotRef> comparator;
    private final ConcurrentSkipListSet<CmpSnapshotRef> index;
    
    SkipListSnapshotRefIndex() {
        this.index = new ConcurrentSkipListSet<CmpSnapshotRef>(SkipListSnapshotRefIndex.comparator);
    }
    
    @Override
    public void add(final BufWindow owner, final Snapshot sn, final ReferenceQueue<? super Snapshot> q) {
        final CmpSnapshotRef ref = new CmpSnapshotRef(sn.vHead, owner, sn, q);
        this.index.add(ref);
    }
    
    @Override
    public long removeAndGetOldest(final SnapshotRef ref) {
        this.index.remove(ref);
        try {
            return this.index.first().head;
        }
        catch (NoSuchElementException e) {
            return -1L;
        }
    }
    
    @Override
    public List<? extends SnapshotRef> toList() {
        return new ArrayList<SnapshotRef>(this.index);
    }
    
    @Override
    public boolean isEmpty() {
        return this.index.isEmpty();
    }
    
    @Override
    public String toString() {
        return this.index.toString();
    }
    
    @Override
    public int size() {
        return this.index.size();
    }
    
    static {
        factory = new SnapshotRefIndexFactory() {
            @Override
            public SnapshotRefIndex create() {
                return new SkipListSnapshotRefIndex();
            }
        };
        comparator = new Comparator<CmpSnapshotRef>() {
            @Override
            public int compare(final CmpSnapshotRef o1, final CmpSnapshotRef o2) {
                if (o1.head < o2.head) {
                    return -1;
                }
                if (o1.head > o2.head) {
                    return 1;
                }
                if (o1.id < o2.id) {
                    return -1;
                }
                if (o1.id > o2.id) {
                    return 1;
                }
                return 0;
            }
        };
    }
    
    private static class CmpSnapshotRef extends SnapshotRef
    {
        private static volatile int next_id;
        private final int id;
        private final long head;
        
        CmpSnapshotRef(final long head, final BufWindow buffer, final Snapshot referent, final ReferenceQueue<? super Snapshot> q) {
            super(buffer, referent, q);
            this.head = head;
            this.id = CmpSnapshotRef.next_id++;
        }
        
        @Override
        public String toString() {
            return "snapshotRef(" + this.head + "," + this.id + "=" + (this).get() + ")";
        }
        
        static {
            CmpSnapshotRef.next_id = 0;
        }
    }
}
