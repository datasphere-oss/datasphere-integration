package com.datasphere.recovery;

public class BaseReaderSourcePosition extends SourcePosition
{
    private static final long serialVersionUID = 5501761434305045684L;
    public final CheckpointDetail recordCheckpoint;
    
    public BaseReaderSourcePosition() {
        this.recordCheckpoint = null;
    }
    
    public BaseReaderSourcePosition(final CheckpointDetail checkpoint) {
        this.recordCheckpoint = checkpoint;
    }
    
    @Override
    public int compareTo(final SourcePosition arg0) {
        final BaseReaderSourcePosition that = (BaseReaderSourcePosition)arg0;
        return this.recordCheckpoint.compareTo(that.recordCheckpoint);
    }
    
    @Override
    public String toString() {
        return this.recordCheckpoint.toString();
    }
    
    @Override
    public String toHumanReadableString() {
        return this.recordCheckpoint.toHumanReadableString();
    }
}
