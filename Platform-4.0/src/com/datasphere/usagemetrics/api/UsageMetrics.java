package com.datasphere.usagemetrics.api;

import java.io.*;

class UsageMetrics implements Serializable
{
    private static final long serialVersionUID = 723626527251618839L;
    public final String namespace;
    public final String source;
    public final long sourceBytes;
    
    UsageMetrics(final String namespace, final String source, final long sourceBytes) {
        this.namespace = namespace;
        this.source = source;
        this.sourceBytes = sourceBytes;
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        final UsageMetrics other = (UsageMetrics)obj;
        return this.namespace.equals(other.namespace) && this.source.equals(other.source) && this.sourceBytes == other.sourceBytes;
    }
    
    @Override
    public int hashCode() {
        int result = (this.namespace != null) ? this.namespace.hashCode() : 0;
        result = 31 * result + ((this.source != null) ? this.source.hashCode() : 0);
        result = 31 * result + new Long(this.sourceBytes).hashCode();
        return result;
    }
    
    @Override
    public String toString() {
        return "UsageMetrics{" + this.namespace + '.' + this.source + '=' + this.sourceBytes + '}';
    }
}
