package com.datasphere.proc.monitor.objects;

import org.codehaus.jackson.map.annotate.*;
import org.codehaus.jackson.annotate.*;

public class CDCMonitorEvent
{
    @JsonView({ MonView.Others.class })
    private String lastCheckPoint;
    @JsonView({ MonView.CDC.class })
    private long noOfInserts;
    @JsonView({ MonView.CDC.class })
    private long noOfUpdates;
    @JsonView({ MonView.CDC.class })
    private long noOfDeletes;
    @JsonView({ MonView.Others.class })
    private long noOfSelects;
    @JsonView({ MonView.CDC.class })
    private long noOfDDLs;
    @JsonView({ MonView.Others.class })
    private String readLag;
    
    public CDCMonitorEvent() {
        this.lastCheckPoint = "";
        this.noOfInserts = 0L;
        this.noOfUpdates = 0L;
        this.noOfDeletes = 0L;
        this.noOfSelects = 0L;
        this.noOfDDLs = 0L;
        this.readLag = "0 ms";
    }
    
    public CDCMonitorEvent(final String lastCheckpoint, final long noOfInserts, final long noOfUpdates, final long noOfDeletes, final String readLag) {
        this.lastCheckPoint = "";
        this.noOfInserts = 0L;
        this.noOfUpdates = 0L;
        this.noOfDeletes = 0L;
        this.noOfSelects = 0L;
        this.noOfDDLs = 0L;
        this.readLag = "0 ms";
        this.lastCheckPoint = lastCheckpoint;
        this.noOfInserts = noOfInserts;
        this.noOfUpdates = noOfUpdates;
        this.noOfDeletes = noOfDeletes;
        this.readLag = readLag;
    }
    
    public String getLastCheckPoint() {
        return this.lastCheckPoint;
    }
    
    public void setLastCheckPoint(final String lastCheckpoint) {
        this.lastCheckPoint = lastCheckpoint;
    }
    
    @JsonProperty("No of Inserts")
    public long getNoOfInserts() {
        return this.noOfInserts;
    }
    
    public void setNo_of_inserts(final long noOfInserts) {
        this.noOfInserts = noOfInserts;
    }
    
    @JsonProperty("No of Updates")
    public long getNoOfUpdates() {
        return this.noOfUpdates;
    }
    
    public void setNoOfUpdates(final long noOfUpdates) {
        this.noOfUpdates = noOfUpdates;
    }
    
    @JsonProperty("No of Deletes")
    public long getNoOfDeletes() {
        return this.noOfDeletes;
    }
    
    public void setNoOfDeletes(final long noOfDeletes) {
        this.noOfDeletes = noOfDeletes;
    }
    
    @JsonProperty("No of Selects")
    public long getNoOfSelects() {
        return this.noOfSelects;
    }
    
    @JsonProperty("No of DDLs")
    public long getNoOfDDLs() {
        return this.noOfDDLs;
    }
    
    public void setNoOfDDLs(final long noOfDDLs) {
        this.noOfDDLs = noOfDDLs;
    }
    
    public void setNoOfSelects(final long noOfSelects) {
        this.noOfSelects = noOfSelects;
    }
    
    public void increment(final CDCType type) {
        switch (type) {
            case INSERT: {
                ++this.noOfInserts;
                break;
            }
            case UPDATE: {
                ++this.noOfUpdates;
                break;
            }
            case DELETE: {
                ++this.noOfDeletes;
                break;
            }
            case SELECT: {
                ++this.noOfSelects;
                break;
            }
            default: {
                ++this.noOfDDLs;
                break;
            }
        }
    }
    
    public String getReadLag() {
        return this.readLag;
    }
    
    public void setReadLag(final String readLag) {
        this.readLag = readLag;
    }
}
