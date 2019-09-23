package com.datasphere.runtime.window;

import java.util.*;
import java.lang.ref.*;

class FibonacciHeapSnapshotRefIndex implements SnapshotRefIndex
{
    public static final SnapshotRefIndexFactory factory;
    private final FibonacciHeap index;
    
    FibonacciHeapSnapshotRefIndex() {
        this.index = new FibonacciHeap();
    }
    
    @Override
    public void add(final BufWindow owner, final Snapshot sn, final ReferenceQueue<? super Snapshot> q) {
        final NodeSnapshotRef ref = new NodeSnapshotRef(sn.vHead, owner, sn, q);
        synchronized (this.index) {
            ref.tag = this.index.insert(ref, sn.vHead);
        }
    }
    
    @Override
    public long removeAndGetOldest(final SnapshotRef ref) {
        assert ref instanceof NodeSnapshotRef;
        final NodeSnapshotRef nref = (NodeSnapshotRef)ref;
        synchronized (this.index) {
            this.index.decreaseKey(nref.tag, -1L);
            final FibonacciHeap.Node r = this.index.removeMin();
            assert r == nref.tag;
            final FibonacciHeap.Node n = this.index.min();
            return (n != null) ? n.getKey() : -1L;
        }
    }
    
    @Override
    public List<? extends SnapshotRef> toList() {
        synchronized (this.index) {
            return this.index.toList();
        }
    }
    
    @Override
    public boolean isEmpty() {
        synchronized (this.index) {
            return this.index.isEmpty();
        }
    }
    
    @Override
    public String toString() {
        synchronized (this.index) {
            return this.index.toString();
        }
    }
    
    @Override
    public int size() {
        return this.index.size();
    }
    
    static {
        factory = new SnapshotRefIndexFactory() {
            @Override
            public SnapshotRefIndex create() {
                return new FibonacciHeapSnapshotRefIndex();
            }
        };
    }
    
    private static class NodeSnapshotRef extends SnapshotRef
    {
        private FibonacciHeap.Node tag;
        
        NodeSnapshotRef(final long head, final BufWindow buffer, final Snapshot referent, final ReferenceQueue<? super Snapshot> q) {
            super(buffer, referent, q);
        }
        
        @Override
        public String toString() {
            return "snapshotRef(" + this.tag.getKey() + "==" + this.get() + ")";
        }
    }
}
