package com.datasphere.rest.apiv2.integration.models;

import java.util.*;

public class TopologyRequest
{
    private String stencilId;
    private String topologyName;
    private Map<String, String> sourceParameters;
    private Map<String, String> targetParameters;
    
    public TopologyRequest() {
        this.stencilId = null;
        this.topologyName = null;
        this.sourceParameters = new HashMap<String, String>();
        this.targetParameters = new HashMap<String, String>();
    }
    
    public TopologyRequest stencilId(final String stencilId) {
        this.stencilId = stencilId;
        return this;
    }
    
    public String getStencilId() {
        return this.stencilId;
    }
    
    public void setStencilId(final String stencilId) {
        this.stencilId = stencilId;
    }
    
    public TopologyRequest topologyName(final String topologyName) {
        this.topologyName = topologyName;
        return this;
    }
    
    public String getTopologyName() {
        return this.topologyName;
    }
    
    public void setTopologyName(final String topologyName) {
        this.topologyName = topologyName;
    }
    
    public TopologyRequest sourceParameters(final Map<String, String> sourceParameters) {
        this.sourceParameters = sourceParameters;
        return this;
    }
    
    public Map<String, String> getSourceParameters() {
        return this.sourceParameters;
    }
    
    public void setSourceParameters(final Map<String, String> sourceParameters) {
        this.sourceParameters = sourceParameters;
    }
    
    public TopologyRequest targetParameters(final Map<String, String> targetParameters) {
        this.targetParameters = targetParameters;
        return this;
    }
    
    public Map<String, String> getTargetParameters() {
        return this.targetParameters;
    }
    
    public void setTargetParameters(final Map<String, String> targetParameters) {
        this.targetParameters = targetParameters;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final TopologyRequest topologyRequest = (TopologyRequest)o;
        return Objects.equals(this.stencilId, topologyRequest.stencilId) && Objects.equals(this.topologyName, topologyRequest.topologyName) && Objects.equals(this.sourceParameters, topologyRequest.sourceParameters) && Objects.equals(this.targetParameters, topologyRequest.targetParameters);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.stencilId, this.topologyName, this.sourceParameters, this.targetParameters);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class TopologyRequest {\n");
        sb.append("    stencilId: ").append(this.toIndentedString(this.stencilId)).append("\n");
        sb.append("    topologyName: ").append(this.toIndentedString(this.topologyName)).append("\n");
        sb.append("    sourceParameters: ").append(this.toIndentedString(this.sourceParameters)).append("\n");
        sb.append("    targetParameters: ").append(this.toIndentedString(this.targetParameters)).append("\n");
        sb.append("}");
        return sb.toString();
    }
    
    private String toIndentedString(final Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}
