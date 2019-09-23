package com.datasphere.health;

import com.datasphere.runtime.monitor.*;

public class Issue implements ComponentHealth, IssueMXBean
{
    public String issue;
    public String fqName;
    public String componentType;
    
    public Issue(final String fqName, final String componentType, final String issue) {
        this.fqName = fqName;
        this.componentType = componentType;
        this.issue = issue;
    }
    
    @Override
    public String getIssue() {
        return this.issue;
    }
    
    @Override
    public String getFqName() {
        return this.fqName;
    }
    
    @Override
    public String getComponentType() {
        return this.componentType;
    }
    
    public void update(final String fqName, final String componentType, final String issue) {
        this.fqName = fqName;
        this.componentType = componentType;
        this.issue = issue;
    }
}
