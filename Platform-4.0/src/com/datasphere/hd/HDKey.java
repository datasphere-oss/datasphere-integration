package com.datasphere.hd;

import java.io.*;
import org.apache.log4j.*;
import com.datasphere.uuid.*;
import javax.jdo.annotations.*;
import flexjson.*;

import com.datasphere.distribution.*;
import com.fasterxml.jackson.annotation.*;

public class HDKey implements Partitionable, Serializable, Comparable<HDKey>
{
    private static Logger logger;
    private static final long serialVersionUID = 250311883592330044L;
    @Persistent
    public UUID id;
    @Persistent
    public Object key;
    
    public HDKey() {
    }
    
    public HDKey(final UUID id, final Object key) {
        this.id = id;
        this.key = key;
    }
    
    public void setId(final String id) {
        this.id = new UUID(id);
    }
    
    public String getId() {
        return this.id.toString();
    }
    
    public void setHDKey(final String hdKeyStr) {
        if (hdKeyStr == null) {
            return;
        }
        final String[] idAndKey = hdKeyStr.split(":", 2);
        this.id = new UUID(idAndKey[0]);
        this.key = ((idAndKey.length == 2) ? idAndKey[1] : this.id);
    }
    
    @JSON(include = false)
    @JsonIgnore
    public String getHDKeyStr() {
        if (this.id != null && this.key != null) {
            return this.id.getUUIDString() + ":" + this.key;
        }
        return null;
    }
    
    public boolean usePartitionId() {
        return false;
    }
    
    @JSON(include = false)
    @JsonIgnore
    public Object getPartitionKey() {
        return this.key;
    }
    
    @JSON(include = false)
    @JsonIgnore
    public int getPartitionId() {
        return 0;
    }
    
    @Override
    public int hashCode() {
        return this.id.hashCode();
    }
    
    @Override
    public boolean equals(final Object obj) {
        return obj instanceof HDKey && this.id.equals((Object)((HDKey)obj).id);
    }
    
    @Override
    public String toString() {
        return "{\"id\":\"" + this.id + "\",\"key\":\"" + this.key + "\"}";
    }
    
    public int compareTo(final HDKey o) {
        return this.id.compareTo(o.id);
    }
    
    static {
        HDKey.logger = Logger.getLogger((Class)HDKey.class);
    }
}
