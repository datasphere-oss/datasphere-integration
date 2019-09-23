package com.datasphere.runtime.window;

import java.util.concurrent.*;

class JumpingTimerPolicy extends BaseTimerPolicy
{
    @Override
    public void startTimer(final ScalaWindow w) {
        this.task = w.getScheduler().scheduleAtFixedRate(w, w.interval, w.interval, TimeUnit.NANOSECONDS);
    }
    
    @Override
    public void onTimer(final ScalaWindow w) throws Exception {
        w.jumpingWindowOnTimerCallback();
    }
    
    @Override
    public void updateWakeupQueue(final ScalaWindow w, final HBuffer b, final long now) {
    }
}
