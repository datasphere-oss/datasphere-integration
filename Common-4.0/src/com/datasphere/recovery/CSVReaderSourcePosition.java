package com.datasphere.recovery;

public class CSVReaderSourcePosition extends SourcePosition
{
    private static final long serialVersionUID = -3461856176551140717L;
    public final CheckpointDetail recordCheckpoint;
    
    public CSVReaderSourcePosition(final CheckpointDetail checkpoint) {
        this.recordCheckpoint = new CheckpointDetail(checkpoint);
    }
    
    @Override
    public int compareTo(final SourcePosition arg0) {
        final CSVReaderSourcePosition that = (CSVReaderSourcePosition)arg0;
        return this.recordCheckpoint.getRecordEndOffset().compareTo(that.recordCheckpoint.getRecordEndOffset());
    }
    
    @Override
    public String toString() {
        return "CSVReader_1_0.Position:" + this.recordCheckpoint;
    }
    
    @Override
    public String toHumanReadableString() {
        return this.recordCheckpoint.toHumanReadableString();
    }
}
