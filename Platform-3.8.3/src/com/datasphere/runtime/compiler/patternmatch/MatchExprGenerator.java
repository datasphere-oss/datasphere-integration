package com.datasphere.runtime.compiler.patternmatch;

import com.datasphere.runtime.compiler.exprs.*;
import com.datasphere.runtime.compiler.select.*;

public interface MatchExprGenerator
{
    void emitCondition(final int p0, final Predicate p1);
    
    void emitVariable(final int p0, final Predicate p1, final DataSet p2);
    
    void emitCreateTimer(final int p0, final long p1);
    
    void emitStopTimer(final int p0, final int p1);
    
    void emitWaitTimer(final int p0, final int p1);
}
