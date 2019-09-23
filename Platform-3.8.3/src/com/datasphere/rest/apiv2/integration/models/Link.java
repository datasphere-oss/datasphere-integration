package com.datasphere.rest.apiv2.integration.models;

import java.util.*;

public class Link
{
    private String rel;
    private List<String> allow;
    private String href;
    
    public Link() {
        this.rel = null;
        this.allow = null;
        this.href = null;
    }
    
    public Link rel(final String rel) {
        this.rel = rel;
        return this;
    }
    
    public String getRel() {
        return this.rel;
    }
    
    public void setRel(final String rel) {
        this.rel = rel;
    }
    
    public Link allow(final List<String> allow) {
        this.allow = allow;
        return this;
    }
    
    public List<String> getAllow() {
        return this.allow;
    }
    
    public void setAllow(final List<String> allow) {
        this.allow = allow;
    }
    
    public Link href(final String href) {
        this.href = href;
        return this;
    }
    
    public String getHref() {
        return this.href;
    }
    
    public void setHref(final String href) {
        this.href = href;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final Link link = (Link)o;
        return Objects.equals(this.rel, link.rel) && Objects.equals(this.allow, link.allow) && Objects.equals(this.href, link.href);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.rel, this.allow, this.href);
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("class Link {\n");
        sb.append("    rel: ").append(this.toIndentedString(this.rel)).append("\n");
        sb.append("    allow: ").append(this.toIndentedString(this.allow)).append("\n");
        sb.append("    href: ").append(this.toIndentedString(this.href)).append("\n");
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
