package com.datasphere.runtime.window;

import java.util.concurrent.*;
import java.util.*;
import com.datasphere.runtime.containers.*;

class JumpingTimeCountWindow extends JumpingWindow
{
    private final long time_interval;
    private final int row_count;
    private Future<?> task;
    private volatile boolean cancelled;
    
    JumpingTimeCountWindow(final BufferManager owner) {
        super(owner);
        this.cancelled = false;
        this.time_interval = this.getPolicy().getTimeInterval();
        this.row_count = this.getPolicy().getRowCount();
        this.setNewWindowSnapshot(this.snapshot);
        this.task = this.schedule(this.time_interval);
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
        final long tail = this.getTail();
        final long nextTail = this.nextHead + this.row_count;
        if (nextTail <= tail) {
            this.task.cancel(true);
            final Snapshot newsn = this.makeSnapshot(this.nextHead, nextTail);
            final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
            this.nextHead = nextTail;
            this.task.cancel(false);
            this.task = this.schedule(this.time_interval);
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
        this.task.cancel(true);
        final Snapshot newsn = this.makeSnapshot(this.nextHead, nextTail);
        final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
        this.nextHead = nextTail;
        this.task.cancel(false);
        this.task = this.schedule(this.time_interval);
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
        this.task = this.schedule(this.time_interval);
        this.notifyOnTimer(newsn, oldEntries);
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
