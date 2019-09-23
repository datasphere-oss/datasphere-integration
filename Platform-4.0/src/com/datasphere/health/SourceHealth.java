package com.datasphere.health;

import com.datasphere.runtime.monitor.*;

public class SourceHealth implements ComponentHealth, SourceHealthMXBean
{
    public long lastEventTime;
    public long eventRate;
    public String fqSourceName;
    
    public SourceHealth(final String fqSourceName) {
        this.fqSourceName = fqSourceName;
    }
    
    public SourceHealth(final String fqSourceName, final long lastEventTime, final long numberOfEvents) {
        this.fqSourceName = fqSourceName;
        this.lastEventTime = lastEventTime;
        this.eventRate = numberOfEvents;
    }
    
    public void update(final String fqSourceName, final long lastEventTime, final long numberOfEvents) {
        this.fqSourceName = fqSourceName;
        this.lastEventTime = lastEventTime;
        this.eventRate = numberOfEvents;
    }
    
    @Override
    public long getLastEventTime() {
        return this.lastEventTime;
    }
    
    @Override
    public long getEventRate() {
        return this.eventRate;
    }
    
    @Override
    public String getFqSourceName() {
        return this.fqSourceName;
    }
}
