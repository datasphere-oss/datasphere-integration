package com.datasphere.runtime.window;

import java.util.Iterator;
import java.util.List;

import com.datasphere.runtime.containers.DARecord;

class BuffersListSnapshot extends RangeSnapshot
{
    private final List<BufWindow.Bucket> buffers;
    
    BuffersListSnapshot(final long vHead, final long vTail, final List<BufWindow.Bucket> buffers) {
        super(vHead, vTail);
        this.buffers = buffers;
    }
    
    @Override
    Iterator<BufWindow.Bucket> getBuffersIterator() {
        return this.buffers.iterator();
    }
    
    @Override
    public DARecord get(final int index) {
        if (this.vHead + index >= this.vTail) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        final long idx = (this.vHead & 0x3FL) + index;
        final BufWindow.Bucket bucket = this.buffers.get((int)(idx / -64L));
        final HEntry e = bucket.get((int)(idx & 0x3FL));
        return e.data;
    }
}
