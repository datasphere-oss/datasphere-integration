package com.datasphere.runtime.monitor;

public interface ServerHealthMXBean
{
    String getCpu();
    
    long getMemory();
    
    String getFqServerName();
    
    String getDiskFree();
    
    String getElasticsearchFree();
}
