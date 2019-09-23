package com.datasphere.rest.apiv2.integration.models;

import com.datasphere.rest.apiv2.appmgmt.models.*;
import java.util.*;

public class Topology
{
    private String topologyName;
    private ApplicationList applicationList;
    private List<Link> links;
    
    public Topology() {
        this.topologyName = null;
        this.applicationList = null;
        this.links = new ArrayList<Link>();
    }
    
    public Topology topologyName(final String topologyName) {
        this.topologyName = topologyName;
        return this;
    }
    
    public String getTopologyName() {
        return this.topologyName;
    }
    
    public void setTopologyName(final String topologyName) {
        this.topologyName = topologyName;
    }
    
    public Topology applicationList(final ApplicationList applicationList) {
        this.applicationList = applicationList;
        return this;
    }
    
    public ApplicationList getApplicationList() {
        return this.applicationList;
    }
    
    public void setApplicationList(final ApplicationList applicationList) {
        this.applicationList = applicationList;
    }
    
    public Topology links(final List<Link> links) {
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
        final Topology topologyRequest = (Topology)o;
        return Objects.equals(this.topologyName, topologyRequest.topologyName) && Objects.equals(this.applicationList, topologyRequest.applicationList);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.topologyName, this.applicationList);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class Topology {\n");
        sb.append("    topologyName: ").append(this.toIndentedString(this.topologyName)).append("\n");
        sb.append("    applicationLit: ").append(this.toIndentedString(this.applicationList)).append("\n");
        sb.append("    links: ").append(this.toIndentedString(this.links)).append("\n");
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
