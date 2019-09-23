package com.datasphere.runtime.monitor;

public interface ClusterInfoMBean
{
    String getId();
    
    long getStartTime();
    
    long getEndTime();
    
    int getClusterSize();
    
    boolean isDerbyAlive();
    
    Object getElasticSearch();
    
    int getAgentCount();
}
