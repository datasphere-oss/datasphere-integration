package com.datasphere.runtime.window;

interface HTimerPolicy
{
    void startTimer(final ScalaWindow p0);
    
    void stopTimer(final ScalaWindow p0);
    
    void onTimer(final ScalaWindow p0) throws Exception;
    
    void updateWakeupQueue(final ScalaWindow p0, final HBuffer p1, final long p2);
}
