package com.datasphere.runtime;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class CustomThreadFactory implements ThreadFactory
{
    private final AtomicInteger threadNumber;
    private final String threadPoolName;
    private final boolean daemons;
    
    public CustomThreadFactory(final String threadPoolName) {
        this(threadPoolName, false);
    }
    
    public CustomThreadFactory(final String threadPoolName, final boolean daemons) {
        this.threadNumber = new AtomicInteger();
        this.threadPoolName = threadPoolName;
        this.daemons = daemons;
    }
    
    @Override
    public Thread newThread(final Runnable r) {
        final Thread t = new Thread(r, this.threadPoolName + "-" + this.threadNumber.incrementAndGet());
        t.setDaemon(this.daemons);
        return t;
    }
}
