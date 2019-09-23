package com.datasphere.runtime.compiler.patternmatch;

import com.datasphere.runtime.compiler.exprs.*;

public class PatternDefinition
{
    public final String varName;
    public final String streamName;
    public final Predicate predicate;
    
    public PatternDefinition(final String var, final String streamName, final Predicate p) {
        this.varName = var;
        this.streamName = streamName;
        this.predicate = p;
    }
    
    @Override
    public String toString() {
        return this.varName + "=" + ((this.streamName == null) ? "" : this.streamName) + "(" + ((this.predicate == null) ? "" : this.predicate) + ")";
    }
}
