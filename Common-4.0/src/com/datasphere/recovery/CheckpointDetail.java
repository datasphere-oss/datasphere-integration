package com.datasphere.recovery;

import java.io.*;
import org.apache.log4j.*;
import java.text.*;
import java.util.*;

public class CheckpointDetail implements Serializable, Comparable<CheckpointDetail>
{
    private static final long serialVersionUID = 5177066802079053229L;
    private Long seekPosition;
    private Long creationTime;
    private Long recordBeginOffset;
    private Long recordEndOffset;
    private Long recordLength;
    private String sourceName;
    private Boolean recovery;
    private long bytesRead;
    private String actualName;
    
    public String getStringVal() {
        final StringBuilder result = new StringBuilder();
        result.append(this.seekPosition);
        result.append(",");
        result.append(this.creationTime);
        result.append(",");
        result.append(this.recordBeginOffset);
        result.append(",");
        result.append(this.recordEndOffset);
        result.append(",");
        result.append(this.recordLength);
        result.append(",");
        result.append(this.bytesRead);
        result.append(",");
        result.append(this.recovery);
        result.append(",");
        result.append(this.sourceName);
        result.append(",");
        result.append(this.actualName);
        return result.toString();
    }
    
    public void setStringVal(final String instr) {
        final String[] parts = instr.split(",");
        this.seekPosition = Long.parseLong(parts[0]);
        this.creationTime = Long.parseLong(parts[1]);
        this.recordBeginOffset = Long.parseLong(parts[2]);
        this.recordEndOffset = Long.parseLong(parts[3]);
        this.recordLength = Long.parseLong(parts[4]);
        this.bytesRead = Long.parseLong(parts[5]);
        this.recovery = Boolean.parseBoolean(parts[6]);
        this.sourceName = parts[7];
        if (parts.length == 9) {
            this.actualName = parts[8];
        }
    }
    
    public CheckpointDetail() {
        this.seekPosition = 0L;
        this.creationTime = 0L;
        this.recordBeginOffset = 0L;
        this.recordEndOffset = 0L;
        this.recordLength = 0L;
        this.sourceName = "";
        this.recovery = false;
        this.bytesRead = 0L;
        this.actualName = "";
    }
    
    public CheckpointDetail(final CheckpointDetail obj) {
        this.seekPosition = 0L;
        this.creationTime = 0L;
        this.recordBeginOffset = 0L;
        this.recordEndOffset = 0L;
        this.recordLength = 0L;
        this.sourceName = "";
        this.recovery = false;
        this.bytesRead = 0L;
        this.actualName = "";
        if (obj == null) {
            return;
        }
        this.seekPosition = obj.seekPosition;
        this.creationTime = obj.creationTime;
        this.recordBeginOffset = obj.recordBeginOffset;
        this.recordEndOffset = obj.recordEndOffset;
        this.recordLength = obj.recordLength;
        this.sourceName = obj.sourceName;
        this.recovery = obj.recovery;
        this.bytesRead = obj.bytesRead;
        this.actualName = obj.actualName;
    }
    
    public void seekPosition(final long seekPosition) {
        this.seekPosition = seekPosition;
    }
    
    public Long seekPosition() {
        return this.seekPosition;
    }
    
    public void setSourceCreationTime(final long creationTime) {
        this.creationTime = creationTime;
    }
    
    public Long getSourceCreationTime() {
        return this.creationTime;
    }
    
    public void setRecordBeginOffset(final long recordBeginOffset) {
        this.recordBeginOffset = recordBeginOffset;
    }
    
    public Long getRecordBeginOffset() {
        return this.recordBeginOffset;
    }
    
    public void setRecordEndOffset(final long recordEndOffset) {
        this.recordEndOffset = recordEndOffset;
    }
    
    public Long getRecordEndOffset() {
        return this.recordEndOffset;
    }
    
    public void setRecordLength(final long recordLength) {
        this.recordLength = recordLength;
    }
    
    public Long getRecordLength() {
        return this.recordLength;
    }
    
    public void setSourceName(final String sourceName) {
        this.sourceName = sourceName;
    }
    
    public String getSourceName() {
        return this.sourceName;
    }
    
    public void setActualName(final String actualName) {
        this.actualName = actualName;
    }
    
    public String getActualName() {
        return this.actualName;
    }
    
    public void setRecovery(final boolean recovery) {
        this.recovery = recovery;
    }
    
    public void setBytesRead(final long bytesRead) {
        this.bytesRead = bytesRead;
    }
    
    public long getBytesRead() {
        return this.bytesRead;
    }
    
    public boolean isRecovery() {
        return this.recovery;
    }
    
    public void dump() {
        final Logger logger = Logger.getLogger((Class)CheckpointDetail.class);
        if (logger.isTraceEnabled()) {
            logger.trace((Object)"*********** Checkpoint Details ****************");
            logger.trace((Object)("seekPosition : [" + this.seekPosition + "]"));
            logger.trace((Object)("creationTime : [" + this.creationTime + "]"));
            logger.trace((Object)("recordBeginOffset : [" + this.recordBeginOffset + "]"));
            logger.trace((Object)("recordEndOffset : [" + this.recordEndOffset + "]"));
            logger.trace((Object)("recordLength : [" + this.recordLength + "]"));
            logger.trace((Object)("sourceName : [" + this.sourceName + "]"));
            logger.trace((Object)("actualName : [" + this.actualName + "]"));
            logger.trace((Object)"***********************************************");
        }
    }
    
    @Override
    public String toString() {
        return this.getStringVal();
    }
    
    public String toHumanReadableString() {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
        final Date creationDateDate = new Date(this.creationTime);
        final String creationDateString = sdf.format(creationDateDate);
        return "*********** Checkpoint Details ***********\n* Seek Position: " + this.seekPosition + "\n* Creation Time: " + creationDateString + "\n* Offset Begin : " + String.format("%,d", this.recordBeginOffset) + "\n* Offset End   : " + String.format("%,d", this.recordEndOffset) + "\n* Record Length: " + String.format("%,d", this.recordLength) + "\n* Source Name  : " + this.sourceName + "\n* Actual name  : " + this.actualName + "\n******************************************";
    }
    
    @Override
    public int compareTo(final CheckpointDetail arg0) {
        int retValue = 0;
        if ((retValue = this.sourceName.compareTo(arg0.getSourceName())) == 0) {
            retValue = (int)(this.getRecordEndOffset() - arg0.getRecordEndOffset());
        }
        return retValue;
    }
}
