package com.datasphere.runtime.window;

import java.util.concurrent.*;

abstract class BaseTimerPolicy implements HTimerPolicy
{
    Future<?> task;
    
    @Override
    public void stopTimer(final ScalaWindow w) {
        if (this.task != null) {
            this.task.cancel(true);
            this.task = null;
        }
    }
}
