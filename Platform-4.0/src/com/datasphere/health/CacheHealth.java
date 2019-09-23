package com.datasphere.health;

import com.datasphere.runtime.monitor.*;

public class CacheHealth implements ComponentHealth, CacheHealthMXBean
{
    public long size;
    public String fqCacheName;
    public long lastRefresh;
    
    public CacheHealth(final String fqCacheName) {
        this.fqCacheName = fqCacheName;
    }
    
    public CacheHealth(final long size, final long lastRefresh, final String fqCacheName) {
        this.size = size;
        this.lastRefresh = lastRefresh;
        this.fqCacheName = fqCacheName;
    }
    
    public void update(final long size, final long lastRefresh, final String fqCacheName) {
        this.size = size;
        this.lastRefresh = lastRefresh;
        this.fqCacheName = fqCacheName;
    }
    
    @Override
    public long getSize() {
        return this.size;
    }
    
    @Override
    public String getFqCacheName() {
        return this.fqCacheName;
    }
    
    @Override
    public long getLastRefresh() {
        return this.lastRefresh;
    }
}
