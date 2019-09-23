package com.datasphere.runtime.monitor;

public interface CacheHealthMXBean
{
    long getSize();
    
    String getFqCacheName();
    
    long getLastRefresh();
}
