package com.datasphere.runtime.window;

import java.util.Iterator;

import com.datasphere.runtime.containers.DARecord;

abstract class RangeSnapshot extends Snapshot
{
    protected final long vTail;
    
    abstract Iterator<BufWindow.Bucket> getBuffersIterator();
    
    RangeSnapshot(final long vHead, final long vTail) {
        super(vHead);
        this.vTail = vTail;
        assert vHead <= vTail;
    }
    
    @Override
    public Iterator<HEntry> itemIterator() {
        return new WEntryIterator(this.getBuffersIterator());
    }
    
    @Override
    public int size() {
        return (int)(this.vTail - this.vHead);
    }
    
    @Override
    public Iterator<DARecord> iterator() {
        return new DataIterator(this.itemIterator());
    }
    
    @Override
    public abstract DARecord get(final int p0);
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        final Iterator<DARecord> it = this.iterator();
        String sep = "";
        while (it.hasNext()) {
            sb.append(sep + it.next());
            sep = ",";
        }
        sb.append("]");
        return "{vHead=" + this.vHead + " vTail=" + this.vTail + " els=" + (Object)sb + "}";
    }
    
    private class DataIterator implements Iterator<DARecord>
    {
        private final Iterator<HEntry> it;
        
        DataIterator(final Iterator<HEntry> it) {
            this.it = it;
        }
        
        @Override
        public boolean hasNext() {
            return this.it.hasNext();
        }
        
        @Override
        public DARecord next() {
            final HEntry e = this.it.next();
            final DARecord o = e.data;
            return o;
        }
        
        @Override
        public void remove() {
            this.it.remove();
        }
    }
    
    private class WEntryIterator implements Iterator<HEntry>
    {
        private final Iterator<BufWindow.Bucket> it;
        private long idx;
        private BufWindow.Bucket bucket;
        
        WEntryIterator(final Iterator<BufWindow.Bucket> it) {
            this.it = it;
            this.idx = RangeSnapshot.this.vHead;
        }
        
        @Override
        public boolean hasNext() {
            return this.idx < RangeSnapshot.this.vTail;
        }
        
        @Override
        public HEntry next() {
            assert this.idx < RangeSnapshot.this.vTail;
            if ((this.idx & 0x3FL) == 0x0L || this.idx == RangeSnapshot.this.vHead) {
                this.bucket = this.it.next();
            }
            final HEntry e = this.bucket.get((int)(this.idx & 0x3FL));
            if (e == null) {
                throw new RuntimeException("NULL WEntry in " + RangeSnapshot.this.getClass().getName() + " snapshot  (" + RangeSnapshot.this.vHead + "-" + RangeSnapshot.this.vTail + "=" + (RangeSnapshot.this.vTail - RangeSnapshot.this.vHead) + ") index = " + this.idx + " bucket " + this.bucket);
            }
            ++this.idx;
            return e;
        }
        
        @Override
        public void remove() {
        }
    }
}
