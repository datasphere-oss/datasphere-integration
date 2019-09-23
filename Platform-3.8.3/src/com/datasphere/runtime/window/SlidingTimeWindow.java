package com.datasphere.runtime.window;

import java.util.concurrent.*;
import com.datasphere.runtime.containers.*;
import java.util.*;

class SlidingTimeWindow extends BufWindow
{
    protected final long time_interval;
    protected volatile Future<?> task;
    private volatile boolean cancelled;
    
    SlidingTimeWindow(final BufferManager owner) {
        super(owner);
        this.cancelled = false;
        this.time_interval = this.getPolicy().getTimeInterval();
    }
    
    @Override
    protected synchronized void update(final IBatch newEntries) {
        final long tail = this.getTail();
        final Snapshot newsn = this.makeSnapshot(this.snapshot.vHead, tail);
        final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
        if (this.task == null) {
            this.task = this.getScheduler().schedule(this, this.time_interval, TimeUnit.NANOSECONDS);
        }
        this.notifyOnUpdate(newEntries, oldEntries);
    }
    
    @Override
    protected void flush() {
        this.snapshot = this.makeEmptySnapshot();
    }
    
    @Override
    protected synchronized void onTimer() {
        if (this.cancelled) {
            return;
        }
        HEntry eHead = null;
        final long curtime = System.nanoTime();
        final Iterator<HEntry> it = this.snapshot.itemIterator();
        while (it.hasNext()) {
            final HEntry e = it.next();
            if (e.timestamp + this.time_interval + 50000L > curtime) {
                eHead = e;
                break;
            }
        }
        final long tail = this.getTail();
        final long newHead = (eHead == null) ? tail : eHead.id;
        final Snapshot newsn = this.makeSnapshot(newHead, tail);
        final List<DARecord> oldEntries = this.setNewWindowSnapshot(newsn);
        final long leftToWait = (eHead != null) ? (eHead.timestamp + this.time_interval - curtime) : this.time_interval;
        this.task = this.schedule(leftToWait);
        if (!oldEntries.isEmpty()) {
            this.notifyOnTimer(Collections.emptyList(), oldEntries);
        }
    }
    
    @Override
    void cancel() {
        if (this.task != null) {
            this.cancelled = true;
            this.task.cancel(true);
        }
        super.cancel();
    }
}
