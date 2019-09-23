package com.datasphere.runtime.window;

import java.util.concurrent.*;
import java.util.*;
import com.datasphere.runtime.containers.*;

abstract class SubMapSnapshot extends RangeSnapshot
{
    private final ConcurrentNavigableMap<Long, BufWindow.Bucket> elements;
    private transient ConcurrentNavigableMap<Long, BufWindow.Bucket> range;
    
    private ConcurrentNavigableMap<Long, BufWindow.Bucket> getRange() {
        if (this.range == null) {
            this.range = new ConcurrentSkipListMap<Long, BufWindow.Bucket>(this.elements.subMap(Long.valueOf(this.vHead & 0xFFFFFFFFFFFFFFC0L), Long.valueOf(this.vTail)));
        }
        return this.range;
    }
    
    SubMapSnapshot(final long vHead, final long vTail, final ConcurrentNavigableMap<Long, BufWindow.Bucket> els) {
        super(vHead, vTail);
        this.elements = els;
    }
    
    public Iterator<BufWindow.Bucket> getBuffersIterator() {
        return this.getRange().values().iterator();
    }
    
    @Override
    public DARecord get(final int index) {
        final long idx = this.vHead + index;
        if (idx >= this.vTail) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        final BufWindow.Bucket bucket = this.getRange().get(idx & 0xFFFFFFFFFFFFFFC0L);
        final HEntry e = bucket.get((int)(idx & 0x3FL));
        return e.data;
    }
}
