package com.datasphere.usagemetrics.persistence.jpa;

import java.sql.*;
import javax.persistence.*;
import java.util.*;

@Entity
@IdClass(UsageMetricsPK.class)
@Table(name = "UsageMetrics")
public class UsageMetrics
{
    private String sourceId;
    private String partitionId;
    private Timestamp recorded;
    private byte[] position;
    private long sourceBytes;
    private String checksum;
    
    @Id
    @Column(name = "SourceID", nullable = false, insertable = true, updatable = false, length = 36, precision = 0)
    public String getSourceId() {
        return this.sourceId;
    }
    
    public void setSourceId(final String sourceId) {
        this.sourceId = sourceId;
    }
    
    @Id
    @Column(name = "PartitionID", nullable = false, insertable = true, updatable = false, length = 36, precision = 0)
    public String getPartitionId() {
        return this.partitionId;
    }
    
    public void setPartitionId(final String partitionId) {
        this.partitionId = partitionId;
    }
    
    @Id
    @Column(name = "Recorded", nullable = false, insertable = true, updatable = false, length = 19, precision = 0)
    public Timestamp getRecorded() {
        return this.recorded;
    }
    
    public void setRecorded(final Timestamp recorded) {
        this.recorded = recorded;
    }
    
    @Basic
    @Column(name = "Position", nullable = true, insertable = true, updatable = false, length = 4096, precision = 0)
    public byte[] getPosition() {
        return this.position;
    }
    
    public void setPosition(final byte[] position) {
        this.position = position.clone();
    }
    
    @Basic
    @Column(name = "SourceBytes", nullable = false, insertable = true, updatable = false, length = 19, precision = 0)
    public long getSourceBytes() {
        return this.sourceBytes;
    }
    
    public void setSourceBytes(final long sourceBytes) {
        this.sourceBytes = sourceBytes;
    }
    
    public long addSourceBytes(final long sourceBytes) {
        return this.sourceBytes += sourceBytes;
    }
    
    @Basic
    @Column(name = "Checksum", nullable = false, insertable = true, updatable = false, length = 40, precision = 0)
    public String getChecksum() {
        return this.checksum;
    }
    
    public void setChecksum(final String checksum) {
        this.checksum = checksum;
    }
    
    @Override
    public int hashCode() {
        int result = (this.sourceId != null) ? this.sourceId.hashCode() : 0;
        result = 31 * result + ((this.partitionId != null) ? this.partitionId.hashCode() : 0);
        result = 31 * result + ((this.recorded != null) ? this.recorded.hashCode() : 0);
        result = 31 * result + ((this.position != null) ? Arrays.hashCode(this.position) : 0);
        result = 31 * result + (int)(this.sourceBytes ^ this.sourceBytes >>> 32);
        result = 31 * result + ((this.checksum != null) ? this.checksum.hashCode() : 0);
        return result;
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        final UsageMetrics that = (UsageMetrics)obj;
        if (this.sourceBytes != that.sourceBytes) {
            return false;
        }
        Label_0076: {
            if (this.sourceId != null) {
                if (this.sourceId.equals(that.sourceId)) {
                    break Label_0076;
                }
            }
            else if (that.sourceId == null) {
                break Label_0076;
            }
            return false;
        }
        Label_0109: {
            if (this.checksum != null) {
                if (this.checksum.equals(that.checksum)) {
                    break Label_0109;
                }
            }
            else if (that.checksum == null) {
                break Label_0109;
            }
            return false;
        }
        Label_0142: {
            if (this.partitionId != null) {
                if (this.partitionId.equals(that.partitionId)) {
                    break Label_0142;
                }
            }
            else if (that.partitionId == null) {
                break Label_0142;
            }
            return false;
        }
        if (!Arrays.equals(this.position, that.position)) {
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
}
