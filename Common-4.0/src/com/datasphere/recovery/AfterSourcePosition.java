package com.datasphere.recovery;

import com.fasterxml.jackson.annotation.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
public class AfterSourcePosition extends SourcePosition
{
    private static final long serialVersionUID = 1657905561025367794L;
    private final SourcePosition sourcePosition;
    
    public AfterSourcePosition(final SourcePosition sourcePosition) {
        this.sourcePosition = sourcePosition;
    }
    
    @Override
    public int compareTo(final SourcePosition arg0) {
        return this.sourcePosition.compareTo(arg0);
    }
    
    public SourcePosition getSourcePosition() {
        return this.sourcePosition;
    }
    
    @Override
    public String toString() {
        return "AFTER:{ " + this.sourcePosition + " }";
    }
}
