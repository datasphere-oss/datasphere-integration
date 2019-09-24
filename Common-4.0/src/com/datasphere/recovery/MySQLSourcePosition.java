package com.datasphere.recovery;

import org.apache.log4j.*;

public class MySQLSourcePosition extends SourcePosition
{
    private static final long serialVersionUID = 291836330502171234L;
    private Long binLogPosition;
    private String binLogName;
    private String previousBinlogName;
    private Integer rowSequenceCount;
    private Long beginRecordPosition;
    private static final Logger logger;
    
    public MySQLSourcePosition() {
        this.binLogPosition = 0L;
        this.binLogName = null;
        this.previousBinlogName = null;
        this.rowSequenceCount = 0;
        this.beginRecordPosition = 0L;
    }
    
    public MySQLSourcePosition(final String name, final long position, final int count, final String previousBinlogName, final Long txnBeginPos) {
        this.binLogPosition = 0L;
        this.binLogName = null;
        this.previousBinlogName = null;
        this.rowSequenceCount = 0;
        this.beginRecordPosition = 0L;
        this.binLogName = name;
        this.binLogPosition = position;
        this.rowSequenceCount = count;
        this.previousBinlogName = previousBinlogName;
        this.beginRecordPosition = txnBeginPos;
    }
    
    @Override
    public int compareTo(final SourcePosition arg0) {
        final MySQLSourcePosition that = (MySQLSourcePosition)arg0;
        if (!this.binLogName.equals(that.binLogName)) {
            final String thisSeqStr = this.binLogName.substring(this.binLogName.lastIndexOf(".") + 1);
            final String thatSeqStr = that.binLogName.substring(that.binLogName.lastIndexOf(".") + 1);
            final Long thisLogIndex = Long.valueOf(thisSeqStr);
            final Long thatLogIndex = Long.valueOf(thatSeqStr);
            final int compare = thisLogIndex.compareTo(thatLogIndex);
            if (compare < 0) {
                final String thisPreviousSeq = this.previousBinlogName.substring(this.previousBinlogName.lastIndexOf(".") + 1);
                final Long thisPreviousSeqIndex = Long.valueOf(thisPreviousSeq);
                final int sequenceCompare = thisPreviousSeqIndex.compareTo(thatLogIndex);
                if (sequenceCompare == 0) {
                    return 1;
                }
            }
            return compare;
        }
        final int compare2 = this.binLogPosition.compareTo(that.binLogPosition);
        if (compare2 == 0) {
            return this.rowSequenceCount.compareTo(that.rowSequenceCount);
        }
        return compare2;
    }
    
    @Override
    public String toString() {
        final String position = "BinlogName : " + this.binLogName + "\nBinLogPosition : " + this.binLogPosition + "\nPreviousBinlogName : " + this.previousBinlogName + "\nRestartPosition : " + this.beginRecordPosition;
        return position;
    }
    
    public String getBinLogName() {
        return this.binLogName;
    }
    
    public String getPreviousBinLogName() {
        return this.previousBinlogName;
    }
    
    public long getRestartPosition() {
        return this.beginRecordPosition;
    }
    
    static {
        logger = Logger.getLogger((Class)MySQLSourcePosition.class);
    }
}
