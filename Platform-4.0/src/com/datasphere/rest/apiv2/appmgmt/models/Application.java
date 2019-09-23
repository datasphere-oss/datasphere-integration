package com.datasphere.rest.apiv2.appmgmt.models;

import java.util.*;

public class Application
{
    private String namespace;
    private String name;
    private String status;
    private List<Link> links;
    
    public Application() {
        this.namespace = null;
        this.name = null;
        this.status = null;
        this.links = new ArrayList<Link>();
    }
    
    public Application namespace(final String namespace) {
        this.namespace = namespace;
        return this;
    }
    
    public String getNamespace() {
        return this.namespace;
    }
    
    public void setNamespace(final String namespace) {
        this.namespace = namespace;
    }
    
    public Application name(final String name) {
        this.name = name;
        return this;
    }
    
    public String getName() {
        return this.name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
    public Application status(final String status) {
        this.status = status;
        return this;
    }
    
    public String getStatus() {
        return this.status;
    }
    
    public void setStatus(final String status) {
        this.status = status;
    }
    
    public Application links(final List<Link> links) {
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
        final Application application = (Application)o;
        return Objects.equals(this.namespace, application.namespace) && Objects.equals(this.name, application.name) && Objects.equals(this.status, application.status) && Objects.equals(this.links, application.links);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.namespace, this.name, this.status, this.links);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class Application {\n");
        sb.append("    namespace: ").append(this.toIndentedString(this.namespace)).append("\n");
        sb.append("    name: ").append(this.toIndentedString(this.name)).append("\n");
        sb.append("    status: ").append(this.toIndentedString(this.status)).append("\n");
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
