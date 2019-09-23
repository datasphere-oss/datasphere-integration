package com.datasphere.runtime;

import java.util.concurrent.*;

public interface ServerServices
{
    ScheduledExecutorService getScheduler();
    
    ExecutorService getThreadPool();
    
    ScheduledFuture<?> scheduleStatsReporting(final Runnable p0, final int p1, final boolean p2);
}
