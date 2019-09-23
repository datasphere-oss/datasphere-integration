package com.datasphere.runtime.monitor;

public interface HStoreHealthMXBean
{
    long getLastWriteTime();
    
    long getWriteRate();
    
    String getFqWAStoreName();
}
