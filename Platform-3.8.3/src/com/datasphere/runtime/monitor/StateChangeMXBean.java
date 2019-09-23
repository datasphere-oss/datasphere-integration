package com.datasphere.runtime.monitor;

public interface StateChangeMXBean
{
    String getFqName();
    
    String getType();
    
    String getPreviousStatus();
    
    String getCurrentStatus();
    
    long getTimestamp();
}
