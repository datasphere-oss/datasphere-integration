package com.datasphere.health;

import com.datasphere.runtime.monitor.*;

public class ServerHealth implements ComponentHealth, ServerHealthMXBean
{
    public String cpu;
    public long memory;
    public String fqServerName;
    public String diskFree;
    public String elasticsearchFree;
    
    public ServerHealth(final String fqServerName) {
        this.fqServerName = fqServerName;
    }
    
    public ServerHealth(final String cpu, final String fqServerName, final long memory, final String diskFree, final String elasticsearchFree) {
        this.cpu = cpu;
        this.fqServerName = fqServerName;
        this.memory = memory;
        this.diskFree = diskFree;
        this.elasticsearchFree = elasticsearchFree;
    }
    
    public void update(final String cpu, final String fqServerName, final long memory, final String diskFree, final String elasticsearchFree) {
        this.cpu = cpu;
        this.fqServerName = fqServerName;
        this.memory = memory;
        this.diskFree = diskFree;
        this.elasticsearchFree = elasticsearchFree;
    }
    
    @Override
    public String getCpu() {
        return this.cpu;
    }
    
    @Override
    public long getMemory() {
        return this.memory;
    }
    
    @Override
    public String getFqServerName() {
        return this.fqServerName;
    }
    
    @Override
    public String getDiskFree() {
        return this.diskFree;
    }
    
    @Override
    public String getElasticsearchFree() {
        return this.elasticsearchFree;
    }
}
