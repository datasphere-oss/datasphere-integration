package com.datasphere.runtime.window;

import java.util.concurrent.*;
import java.util.*;
import com.datasphere.runtime.containers.*;

class JumpingTimeWindow extends JumpingWindow
{
    protected final long time_interval;
    private Future<?> task;
    private volatile boolean cancelled;
    
    JumpingTimeWindow(final BufferManager owner) {
        super(owner);
        this.cancelled = false;
        this.time_interval = this.getPolicy().getTimeInterval();
        this.nextHead = this.snapshot.vHead;
        this.setNewWindowSnapshot(this.snapshot);
        this.task = this.scheduleAtFixedRate(this.time_interval);
    }
    
    @Override
    protected synchronized void update(final IBatch newEntries) {
        if (this.nextHead == null) {
            this.nextHead = this.getHead();
            if (this.nextHead == null) {
                return;
            }
            this.snapshot = this.makeOneItemSnapshot(this.nextHead);
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
    
    @Override
    protected synchronized void onTimer() {
        if (this.cancelled) {
            return;
        }
        if (this.nextHead == null) {
            this.nextHead = this.getHead();
            if (this.nextHead == null) {
                return;
            }
            this.snapshot = this.makeOneItemSnapshot(this.nextHead);
        }
        final long tail = this.getTail();
        final Snapshot newsn = this.makeSnapshot(this.nextHead, tail);
        this.nextHead = tail;
        final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
        if (!newsn.isEmpty() || !oldEntries.isEmpty()) {
            this.notifyOnTimer(newsn, oldEntries);
        }
        this.snapshot = newsn;
    }
    
    @Override
    void cancel() {
        if (this.task != null) {
            this.cancelled = true;
            this.task.cancel(false);
        }
        super.cancel();
    }
}
