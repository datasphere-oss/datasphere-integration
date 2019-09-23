package com.datasphere.runtime.window;

import com.datasphere.runtime.containers.*;
import java.util.*;

abstract class HBufferJumping extends HBuffer
{
    private List<DARecord> buffer;
    private List<DARecord> oldbuf;
    
    HBufferJumping() {
        this.buffer = new ArrayList<DARecord>();
        this.oldbuf = new ArrayList<DARecord>();
    }
    
    @Override
    final void addEvent(final DARecord e) {
        this.buffer.add(e);
    }
    
    @Override
    final DARecord getFirstEvent() throws NoSuchElementException {
        try {
            return this.buffer.get(0);
        }
        catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }
    }
    
    @Override
    final int getSize() {
        return this.buffer.size();
    }
    
    @Override
    Range makeSnapshot() {
        return Range.createRange(this.key, this.oldbuf);
    }
    
    void doJump() throws Exception {
        final List<DARecord> newrec = this.buffer;
        final List<DARecord> oldrec = this.oldbuf;
        this.oldbuf = this.buffer;
        this.buffer = new ArrayList<DARecord>();
        this.publish(newrec, oldrec);
    }
}
