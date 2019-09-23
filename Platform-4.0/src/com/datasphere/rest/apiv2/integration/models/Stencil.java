package com.datasphere.rest.apiv2.integration.models;

import java.util.*;

public class Stencil
{
    private String stencilName;
    private List<StencilParameter> sourceParameters;
    private List<StencilParameter> targetParameters;
    private List<Link> links;
    
    public Stencil() {
        this.stencilName = null;
        this.sourceParameters = new ArrayList<StencilParameter>();
        this.targetParameters = new ArrayList<StencilParameter>();
        this.links = new ArrayList<Link>();
    }
    
    public Stencil stencilName(final String stencilName) {
        this.stencilName = stencilName;
        return this;
    }
    
    public String getStencilName() {
        return this.stencilName;
    }
    
    public void setStencilName(final String stencilName) {
        this.stencilName = stencilName;
    }
    
    public Stencil sourceParameters(final List<StencilParameter> sourceParameters) {
        this.sourceParameters = sourceParameters;
        return this;
    }
    
    public List<StencilParameter> getSourceParameters() {
        return this.sourceParameters;
    }
    
    public void setSourceParameters(final List<StencilParameter> sourceParameters) {
        this.sourceParameters = sourceParameters;
    }
    
    public Stencil targetParameters(final List<StencilParameter> targetParameters) {
        this.targetParameters = targetParameters;
        return this;
    }
    
    public List<StencilParameter> getTargetParameters() {
        return this.targetParameters;
    }
    
    public void setTargetParameters(final List<StencilParameter> targetParameters) {
        this.targetParameters = targetParameters;
    }
    
    public Stencil links(final List<Link> links) {
        this.links = links;
        return this;
    }
    
    public List<Link> getLinks() {
        return this.links;
    }
    
    public void setLinks(final List<Link> links) {
        this.links = links;
    }
    
    public void addLink(final String rel, final List<String> allow, final String href) {
        final Link link = new Link();
        link.setRel(rel);
        link.setAllow(allow);
        link.setHref(href);
        this.links.add(link);
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final Stencil stencil = (Stencil)o;
        return Objects.equals(this.stencilName, stencil.stencilName) && Objects.equals(this.sourceParameters, stencil.sourceParameters) && Objects.equals(this.targetParameters, stencil.targetParameters);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.stencilName, this.sourceParameters, this.targetParameters);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class Stencil {\n");
        sb.append("    stencilName: ").append(this.toIndentedString(this.stencilName)).append("\n");
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
