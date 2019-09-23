package com.datasphere.usagemetrics.persistence.jpa;

import java.io.*;
import java.sql.*;
import javax.persistence.*;

class UsageMetricsPK implements Serializable
{
    private static final long serialVersionUID = -889659077041659280L;
    private String sourceId;
    private String partitionId;
    private Timestamp recorded;
    
    @Column(name = "SourceID", nullable = false, insertable = true, updatable = false, length = 36, precision = 0)
    @Id
    public String getSourceId() {
        return this.sourceId;
    }
    
    public void setSourceId(final String sourceId) {
        this.sourceId = sourceId;
    }
    
    @Column(name = "PartitionID", nullable = false, insertable = true, updatable = false, length = 36, precision = 0)
    @Id
    public String getPartitionId() {
        return this.partitionId;
    }
    
    public void setPartitionId(final String partitionId) {
        this.partitionId = partitionId;
    }
    
    @Column(name = "Recorded", nullable = false, insertable = true, updatable = false, length = 19, precision = 0)
    @Id
    public Timestamp getRecorded() {
        return this.recorded;
    }
    
    public void setRecorded(final Timestamp recorded) {
        this.recorded = recorded;
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        final UsageMetricsPK that = (UsageMetricsPK)obj;
        Label_0062: {
            if (this.sourceId != null) {
                if (this.sourceId.equals(that.sourceId)) {
                    break Label_0062;
                }
            }
            else if (that.sourceId == null) {
                break Label_0062;
            }
            return false;
        }
        Label_0095: {
            if (this.partitionId != null) {
                if (this.partitionId.equals(that.partitionId)) {
                    break Label_0095;
                }
            }
            else if (that.partitionId == null) {
                break Label_0095;
            }
            return false;
        }
        if (this.recorded != null) {
            if (this.recorded.equals(that.recorded)) {
                return true;
            }
        }
        else if (that.recorded == null) {
            return true;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        int result = (this.sourceId != null) ? this.sourceId.hashCode() : 0;
        result = 31 * result + ((this.partitionId != null) ? this.partitionId.hashCode() : 0);
        result = 31 * result + ((this.recorded != null) ? this.recorded.hashCode() : 0);
        return result;
    }
}
