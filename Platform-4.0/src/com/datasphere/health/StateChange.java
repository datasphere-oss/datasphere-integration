package com.datasphere.health;

import com.datasphere.runtime.monitor.*;
import com.datasphere.runtime.meta.*;

public class StateChange implements ComponentHealth, StateChangeMXBean
{
    public String fqName;
    public String type;
    public String previousStatus;
    public String currentStatus;
    public long timestamp;
    
    public StateChange(final String fqName, final String type, final String previousStatus, final String currentStatus, final long timestamp) {
        this.fqName = fqName;
        this.type = type;
        this.previousStatus = previousStatus;
        this.currentStatus = currentStatus;
        this.timestamp = timestamp;
    }
    
    public StateChange(final MetaInfo.StatusInfo status) {
        this.fqName = status.getOID().getUUIDString();
        this.currentStatus = status.status.name();
        this.previousStatus = status.previousStatus.name();
        this.timestamp = System.currentTimeMillis();
    }
    
    @Override
    public String getFqName() {
        return this.fqName;
    }
    
    @Override
    public String getType() {
        return this.type;
    }
    
    @Override
    public String getPreviousStatus() {
        return this.previousStatus;
    }
    
    @Override
    public String getCurrentStatus() {
        return this.currentStatus;
    }
    
    @Override
    public long getTimestamp() {
        return this.timestamp;
    }
    
    public void update(final String fqName, final String type, final String previousStatus, final String currentStatus, final long timestamp) {
        this.fqName = fqName;
        this.type = type;
        this.previousStatus = previousStatus;
        this.currentStatus = currentStatus;
        this.timestamp = timestamp;
    }
}
