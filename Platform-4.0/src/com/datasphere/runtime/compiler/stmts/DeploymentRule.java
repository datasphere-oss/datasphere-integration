package com.datasphere.runtime.compiler.stmts;

import java.io.*;
import com.datasphere.runtime.*;

public class DeploymentRule implements Serializable
{
    private static final long serialVersionUID = 5003499339179392410L;
    public final DeploymentStrategy strategy;
    public String flowName;
    public final String deploymentGroup;
    
    public DeploymentRule(final DeploymentStrategy strategy, final String flowName, final String deploymentGroup) {
        this.strategy = strategy;
        this.flowName = flowName;
        this.deploymentGroup = deploymentGroup;
    }
    
    @Override
    public String toString() {
        return this.flowName + ((this.strategy == DeploymentStrategy.ON_ONE) ? " ON ONE " : " ON ALL ") + "IN " + this.deploymentGroup + " ";
    }
}
