package com.datasphere.health;

import com.datasphere.runtime.monitor.*;

public class TargetHealth implements ComponentHealth, TargetHealthMXBean
{
    public long lastWriteTime;
    public long eventRate;
    public String fqTargetName;
    
    public TargetHealth(final String fqTargetName) {
        this.fqTargetName = fqTargetName;
    }
    
    public TargetHealth(final String fqTargetName, final long lastWriteTime, final long numberOfEvents) {
        this.fqTargetName = fqTargetName;
        this.lastWriteTime = lastWriteTime;
        this.eventRate = numberOfEvents;
    }
    
    public void update(final String fqTargetName, final long lastWriteTime, final long numberOfEvents) {
        this.fqTargetName = fqTargetName;
        this.lastWriteTime = lastWriteTime;
        this.eventRate = numberOfEvents;
    }
    
    @Override
    public long getLastWriteTime() {
        return this.lastWriteTime;
    }
    
    @Override
    public long getEventRate() {
        return this.eventRate;
    }
    
    @Override
    public String getFqTargetName() {
        return this.fqTargetName;
    }
}
