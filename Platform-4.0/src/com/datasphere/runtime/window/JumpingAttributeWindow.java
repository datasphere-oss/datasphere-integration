package com.datasphere.runtime.window;

import com.datasphere.runtime.containers.*;
import java.util.*;

class JumpingAttributeWindow extends JumpingWindow
{
    private final CmpAttrs attrComparator;
    
    JumpingAttributeWindow(final BufferManager owner) {
        super(owner);
        this.attrComparator = this.getPolicy().getComparator();
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
        if (!newEntries.isEmpty()) {
            final long tail = this.getTail();
            DARecord first = this.getData(this.nextHead);
            assert first != null;
            int n = newEntries.size();
            for (final Object o : newEntries) {
                final DARecord obj = (DARecord)o;
                if (!this.attrComparator.inRange(first, obj)) {
                    final long nextTail = tail - n;
                    final Snapshot newsn = this.makeSnapshot(this.nextHead, nextTail);
                    final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
                    this.nextHead = nextTail;
                    this.notifyOnUpdate(newsn, oldEntries);
                    this.snapshot = newsn;
                    first = obj;
                }
                --n;
            }
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
