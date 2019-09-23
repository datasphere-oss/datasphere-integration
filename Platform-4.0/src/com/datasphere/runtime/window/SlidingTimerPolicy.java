package com.datasphere.runtime.window;

import java.util.*;
import java.util.concurrent.*;

class SlidingTimerPolicy extends BaseTimerPolicy
{
    private boolean firstTime;
    private LinkedList<TimerTask> timeTaskQueue;
    
    SlidingTimerPolicy() {
        this.firstTime = true;
        this.timeTaskQueue = new LinkedList<TimerTask>();
    }
    
    @Override
    public void startTimer(final ScalaWindow w) {
    }
    
    @Override
    public void onTimer(final ScalaWindow w) throws Exception {
        final long now = System.nanoTime();
        long waitTime = w.interval;
        while (!this.timeTaskQueue.isEmpty()) {
            TimerTask t = this.timeTaskQueue.peek();
            if (t.createdTimestamp + w.interval > now) {
                waitTime = t.createdTimestamp + w.interval - now;
                break;
            }
            t = this.timeTaskQueue.remove();
            t.buffer.removeTill(now);
        }
        this.task = w.getScheduler().schedule(w, waitTime, TimeUnit.NANOSECONDS);
    }
    
    @Override
    public void updateWakeupQueue(final ScalaWindow w, final HBuffer b, final long now) {
        this.timeTaskQueue.add(new TimerTask(b, now));
        if (this.firstTime) {
            this.task = w.getScheduler().schedule(w, w.interval, TimeUnit.NANOSECONDS);
            this.firstTime = false;
        }
    }
    
    static class TimerTask
    {
        final HBuffer buffer;
        final long createdTimestamp;
        
        TimerTask(final HBuffer b, final long now) {
            this.buffer = b;
            this.createdTimestamp = now;
        }
    }
}
