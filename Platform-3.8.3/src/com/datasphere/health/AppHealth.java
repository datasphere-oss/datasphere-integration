package com.datasphere.health;

import com.datasphere.runtime.monitor.*;

public class AppHealth implements ComponentHealth, AppHealthMXBean
{
    public String fqAppName;
    public String status;
    public long lastModifiedTime;
    
    public AppHealth(final String fqAppName, final String status, final long lastModifiedTime) {
        this.fqAppName = fqAppName;
        this.status = status;
        this.lastModifiedTime = lastModifiedTime;
    }
    
    @Override
    public String getFqAppName() {
        return this.fqAppName;
    }
    
    @Override
    public String getStatus() {
        return this.status;
    }
    
    @Override
    public long getLastModifiedTime() {
        return this.lastModifiedTime;
    }
    
    public void update(final String fqAppName, final String status, final long lastModifiedTime) {
        this.fqAppName = fqAppName;
        this.status = status;
        this.lastModifiedTime = lastModifiedTime;
    }
}
