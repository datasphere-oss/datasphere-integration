package com.datasphere.recovery;

public class JSONSourcePosition extends SourcePosition
{
    private static final long serialVersionUID = 8373700063769046220L;
    public final CheckpointDetail checkpoint;
    
    public JSONSourcePosition(final CheckpointDetail checkpoint) {
        this.checkpoint = checkpoint;
    }
    
    @Override
    public int compareTo(final SourcePosition arg0) {
        final JSONSourcePosition that = (JSONSourcePosition)arg0;
        return this.checkpoint.getSourceCreationTime().compareTo(that.checkpoint.getSourceCreationTime());
    }
    
    @Override
    public String toString() {
        return this.checkpoint.getStringVal();
    }
}
