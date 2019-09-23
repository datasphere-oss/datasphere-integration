package com.datasphere.rest.apiv2.appmgmt.models;

import java.util.*;

public class FlowDeploymentPlan
{
    private String flowName;
    private String deploymentGroupName;
    private DeploymentTypeEnum deploymentType;
    
    public FlowDeploymentPlan() {
        this.flowName = null;
        this.deploymentGroupName = null;
        this.deploymentType = null;
    }
    
    public FlowDeploymentPlan flowName(final String flowName) {
        this.flowName = flowName;
        return this;
    }
    
    public String getFlowName() {
        return this.flowName;
    }
    
    public void setFlowName(final String flowName) {
        this.flowName = flowName;
    }
    
    public FlowDeploymentPlan deploymentGroupName(final String deploymentGroupName) {
        this.deploymentGroupName = deploymentGroupName;
        return this;
    }
    
    public String getDeploymentGroupName() {
        return this.deploymentGroupName;
    }
    
    public void setDeploymentGroupName(final String deploymentGroupName) {
        this.deploymentGroupName = deploymentGroupName;
    }
    
    public FlowDeploymentPlan deploymentType(final DeploymentTypeEnum deploymentType) {
        this.deploymentType = deploymentType;
        return this;
    }
    
    public DeploymentTypeEnum getDeploymentType() {
        return this.deploymentType;
    }
    
    public void setDeploymentType(final DeploymentTypeEnum deploymentType) {
        this.deploymentType = deploymentType;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final FlowDeploymentPlan flowDeploymentPlan = (FlowDeploymentPlan)o;
        return Objects.equals(this.flowName, flowDeploymentPlan.flowName) && Objects.equals(this.deploymentGroupName, flowDeploymentPlan.deploymentGroupName) && Objects.equals(this.deploymentType, flowDeploymentPlan.deploymentType);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.flowName, this.deploymentGroupName, this.deploymentType);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class FlowDeploymentPlan {\n");
        sb.append("    flowName: ").append(this.toIndentedString(this.flowName)).append("\n");
        sb.append("    deploymentGroupName: ").append(this.toIndentedString(this.deploymentGroupName)).append("\n");
        sb.append("    deploymentType: ").append(this.toIndentedString(this.deploymentType)).append("\n");
        sb.append("}");
        return sb.toString();
    }
    
    private String toIndentedString(final Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
    
    public enum DeploymentTypeEnum
    {
        ANY, 
        ALL;
        
        public String value() {
            return this.name();
        }
        
        public static DeploymentTypeEnum fromValue(final String v) {
            return valueOf(v);
        }
    }
}
