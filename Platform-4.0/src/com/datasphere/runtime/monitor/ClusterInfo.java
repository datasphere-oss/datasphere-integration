package com.datasphere.runtime.monitor;

public class ClusterInfo implements ClusterInfoMBean
{
    String id;
    private long startTime;
    private long endTime;
    private int clusterSize;
    private boolean derbyAlive;
    private Object elasticSearch;
    private int agentCount;
    
    public ClusterInfo() {
        this.clusterSize = -1;
    }
    
    @Override
    public String getId() {
        return this.id;
    }
    
    @Override
    public long getStartTime() {
        return this.startTime;
    }
    
    @Override
    public long getEndTime() {
        return this.endTime;
    }
    
    @Override
    public int getClusterSize() {
        return this.clusterSize;
    }
    
    @Override
    public boolean isDerbyAlive() {
        return this.derbyAlive;
    }
    
    @Override
    public Object getElasticSearch() {
        return this.elasticSearch;
    }
    
    @Override
    public int getAgentCount() {
        return this.agentCount;
    }
    
    public void updateBean(final String id, final long startTime, final long endTime, final int clusterSize, final boolean derbyAlive, final Object elasticSearch, final int agentCount) {
        this.id = id;
        this.startTime = startTime;
        this.endTime = endTime;
        this.clusterSize = clusterSize;
        this.derbyAlive = derbyAlive;
        this.elasticSearch = elasticSearch;
        this.agentCount = agentCount;
    }
}
