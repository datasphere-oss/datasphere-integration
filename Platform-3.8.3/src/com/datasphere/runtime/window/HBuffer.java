package com.datasphere.runtime.window;

import com.datasphere.runtime.*;
import com.datasphere.runtime.containers.*;
import java.util.*;

abstract class HBuffer
{
    RecordKey key;
    private ScalaWindow owner;
    
    void setOwner(final ScalaWindow owner, final RecordKey key) {
        this.owner = owner;
        this.key = key;
    }
    
    final void publish(final List<DARecord> added, final List<DARecord> removed) throws Exception {
        final Range snapshot = this.makeSnapshot();
        this.owner.publish(Batch.asBatch(added), removed, snapshot);
    }
    
    final void publish(final Batch added, final List<DARecord> removed) throws Exception {
        final Range snapshot = this.makeSnapshot();
        this.owner.publish(added, removed, snapshot);
    }
    
    abstract void update(final Batch p0, final long p1) throws Exception;
    
    abstract Range makeSnapshot();
    
    void removeTill(final long now) throws Exception {
        throw new UnsupportedOperationException();
    }
    
    void removeAll() throws Exception {
        throw new UnsupportedOperationException();
    }
    
    abstract void addEvent(final DARecord p0);
    
    abstract DARecord getFirstEvent() throws NoSuchElementException;
    
    abstract int getSize();
    
    final void addEvents(final IBatch added) {
    		Iterator iter = added.iterator();
        while (iter.hasNext()) {
        		final DARecord e = (DARecord)iter.next();
            this.addEvent(e);
        }
    }
    
    final boolean countFull() {
        return this.getSize() == this.owner.count;
    }
    
    final boolean countOverflow() {
        return this.getSize() > this.owner.count;
    }
    
    final boolean notExpiredYet(final long eventTimestamp, final long now) {
        return eventTimestamp + this.owner.interval > now;
    }
    
    final boolean inRange(final DARecord a, final DARecord b) {
        return this.owner.attrComparator.inRange(a, b);
    }
}
