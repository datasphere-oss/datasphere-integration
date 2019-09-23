package com.datasphere.runtime.monitor;

public interface TargetHealthMXBean
{
    long getLastWriteTime();
    
    long getEventRate();
    
    String getFqTargetName();
}
