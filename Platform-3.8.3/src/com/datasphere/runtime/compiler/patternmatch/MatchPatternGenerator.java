package com.datasphere.runtime.compiler.patternmatch;

import java.util.*;

public interface MatchPatternGenerator
{
    void emitAnchor(final ValidatedPatternNode p0, final int p1);
    
    void emitVariable(final ValidatedPatternNode p0, final PatternVariable p1);
    
    void emitRepetition(final ValidatedPatternNode p0, final ValidatedPatternNode p1, final PatternRepetition p2);
    
    void emitAlternation(final ValidatedPatternNode p0, final List<ValidatedPatternNode> p1);
    
    void emitSequence(final ValidatedPatternNode p0, final List<ValidatedPatternNode> p1);
}
