package com.datasphere.health;

import com.datasphere.runtime.monitor.*;

public class HStoreHealth implements ComponentHealth, HStoreHealthMXBean
{
    public long lastWriteTime;
    public long writeRate;
    public String fqWAStoreName;
    
    public HStoreHealth(final String fqWAStoreName) {
        this.fqWAStoreName = fqWAStoreName;
    }
    
    public HStoreHealth(final String fqWAStoreName, final long lastWriteTime, final long numberOfWrites) {
        this.fqWAStoreName = fqWAStoreName;
        this.lastWriteTime = lastWriteTime;
        this.writeRate = numberOfWrites;
    }
    
    public void update(final String fqWAStoreName, final long lastWriteTime, final long numberOfWrites) {
        this.fqWAStoreName = fqWAStoreName;
        this.lastWriteTime = lastWriteTime;
        this.writeRate = numberOfWrites;
    }
    
    @Override
    public long getLastWriteTime() {
        return this.lastWriteTime;
    }
    
    @Override
    public long getWriteRate() {
        return this.writeRate;
    }
    
    @Override
    public String getFqWAStoreName() {
        return this.fqWAStoreName;
    }
}
