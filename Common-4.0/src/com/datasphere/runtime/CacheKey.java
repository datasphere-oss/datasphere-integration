package com.datasphere.runtime;

import java.io.*;

import com.datasphere.distribution.*;

public class CacheKey extends RecordKey implements Partitionable, Serializable
{
    private static final long serialVersionUID = -1425225889130955124L;
    
    public CacheKey(final Object K) {
        this.singleField = K;
        this.fields = null;
    }
    
    public CacheKey(final Object[] arg) {
        super(arg);
    }
    
    @Override
    public boolean usePartitionId() {
        return false;
    }
    
    @Override
    public Object getPartitionKey() {
        return (this.singleField == null) ? this.fields[0] : this.singleField;
    }
    
    @Override
    public int getPartitionId() {
        return this.hashCode();
    }
}
