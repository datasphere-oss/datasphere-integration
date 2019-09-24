package com.datasphere.recovery;

public class AvroCheckpointDetail extends CheckpointDetail
{
    private static final long serialVersionUID = 3544878361207130863L;
    private Long blockCount;
    private Long recordCount;
    private String previousFileName;
    
    public AvroCheckpointDetail(final Long blkCount, final Long recCount, final String fileName, final String previousFileName) {
        this.blockCount = 0L;
        this.recordCount = 0L;
        this.previousFileName = null;
        this.blockCount = blkCount;
        this.recordCount = recCount;
        super.setSourceName(fileName);
        this.previousFileName = previousFileName;
    }
    
    public AvroCheckpointDetail(final CheckpointDetail obj) {
        super(obj);
        this.blockCount = 0L;
        this.recordCount = 0L;
        this.previousFileName = null;
    }
    
    @Override
    public int compareTo(final CheckpointDetail arg0) {
        final AvroCheckpointDetail that = (AvroCheckpointDetail)arg0;
        if (this.getSourceName().equals(that.getSourceName())) {
            final int compare = this.blockCount.compareTo(that.blockCount);
            if (compare == 0) {
                return this.recordCount.compareTo(that.recordCount);
            }
            return compare;
        }
        else {
            if (this.previousFileName.equals(that.getSourceName())) {
                return 1;
            }
            return 0;
        }
    }
    
    public Long getBlockCount() {
        return this.blockCount;
    }
    
    public Long getRecordCount() {
        return this.recordCount;
    }
    
    @Override
    public String toString() {
        return "FileName : " + this.getSourceName() + "\nBlock Count : " + this.blockCount + "\nRecord Count : " + this.recordCount;
    }
}
