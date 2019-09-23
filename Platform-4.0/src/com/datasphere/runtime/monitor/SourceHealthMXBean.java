package com.datasphere.runtime.monitor;

public interface SourceHealthMXBean
{
    long getLastEventTime();
    
    long getEventRate();
    
    String getFqSourceName();
}
