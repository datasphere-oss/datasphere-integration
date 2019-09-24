package com.datasphere.concurrency;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class NamedThreadFactory implements ThreadFactory
{
    private final AtomicInteger id;
    private final String prefix;
    
    public NamedThreadFactory(final String prefix) {
        this.id = new AtomicInteger();
        this.prefix = prefix;
    }
    
    @Override
    public Thread newThread(final Runnable r) {
        final int newThreadId = this.id.getAndIncrement();
        final Thread t = new Thread(r);
        t.setName(this.prefix + "-" + newThreadId);
        return t;
    }
}
