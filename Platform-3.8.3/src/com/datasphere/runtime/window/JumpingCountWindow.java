package com.datasphere.runtime.window;

import java.util.*;
import com.datasphere.runtime.containers.*;

class JumpingCountWindow extends JumpingWindow
{
    private final int row_count;
    
    JumpingCountWindow(final BufferManager owner) {
        super(owner);
        this.row_count = this.getPolicy().getRowCount();
    }
    
    @Override
    protected void update(final IBatch newEntries) {
        if (this.nextHead == null) {
            this.nextHead = this.getHead();
            if (this.nextHead == null) {
                return;
            }
            this.snapshot = this.makeOneItemSnapshot(this.nextHead);
        }
        final long tail = this.getTail();
        final long nextTail = this.nextHead + this.row_count;
        if (nextTail <= tail) {
            final Snapshot newsn = this.makeSnapshot(this.nextHead, nextTail);
            final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
            this.nextHead = nextTail;
            this.notifyOnUpdate(newsn, oldEntries);
            this.snapshot = newsn;
        }
    }
    
    @Override
    protected void flush() {
        if (this.nextHead == null) {
            return;
        }
        final long nextTail = this.getTail();
        final Snapshot newsn = this.makeSnapshot(this.nextHead, nextTail);
        final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
        this.nextHead = nextTail;
        this.notifyOnUpdate(newsn, oldEntries);
        this.snapshot = this.makeEmptySnapshot();
    }
}
