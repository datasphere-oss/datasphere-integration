package com.datasphere.runtime.window;

class NoTimerPolicy implements HTimerPolicy
{
    @Override
    public void startTimer(final ScalaWindow w) {
    }
    
    @Override
    public void stopTimer(final ScalaWindow w) {
    }
    
    @Override
    public void onTimer(final ScalaWindow w) throws Exception {
    }
    
    @Override
    public void updateWakeupQueue(final ScalaWindow w, final HBuffer b, final long now) {
    }
}
