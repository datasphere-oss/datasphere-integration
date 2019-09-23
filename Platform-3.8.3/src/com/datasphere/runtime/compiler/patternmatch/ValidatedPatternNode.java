package com.datasphere.runtime.compiler.patternmatch;

import java.util.*;

public abstract class ValidatedPatternNode
{
    private final int nodeid;
    
    public ValidatedPatternNode(final int id) {
        this.nodeid = id;
    }
    
    public int getID() {
        return this.nodeid;
    }
    
    public abstract void emit(final MatchPatternGenerator p0);
    
    @Override
    public abstract String toString();
    
    public static ValidatedPatternNode makeRestartAnchor(final int nodeid, final int id) {
        return new ValidatedPatternNode(nodeid) {
            @Override
            public void emit(final MatchPatternGenerator g) {
                g.emitAnchor(this, id);
            }
            
            @Override
            public String toString() {
                return "#";
            }
        };
    }
    
    public static ValidatedPatternNode makePatternVariable(final int nodeid, final PatternVariable var) {
        return new ValidatedPatternNode(nodeid) {
            @Override
            public void emit(final MatchPatternGenerator g) {
                g.emitVariable(this, var);
            }
            
            @Override
            public String toString() {
                return var.getName();
            }
        };
    }
    
    public static ValidatedPatternNode makePatternRepetition(final int nodeid, final ValidatedPatternNode subnode, final PatternRepetition repetition) {
        return new ValidatedPatternNode(nodeid) {
            @Override
            public void emit(final MatchPatternGenerator g) {
                g.emitRepetition(this, subnode, repetition);
            }
            
            @Override
            public String toString() {
                return subnode + repetition.toString();
            }
        };
    }
    
    public static ValidatedPatternNode makeAlternation(final int nodeid, final List<ValidatedPatternNode> subnodes) {
        return new ValidatedPatternNode(nodeid) {
            @Override
            public void emit(final MatchPatternGenerator g) {
                g.emitAlternation(this, subnodes);
            }
            
            @Override
            public String toString() {
                final StringBuilder b = new StringBuilder();
                String sep = "";
                for (final ValidatedPatternNode n : subnodes) {
                    b.append(sep).append(n);
                    sep = "|";
                }
                return b.toString();
            }
        };
    }
    
    public static ValidatedPatternNode makeSequence(final int nodeid, final List<ValidatedPatternNode> subnodes) {
        return new ValidatedPatternNode(nodeid) {
            @Override
            public void emit(final MatchPatternGenerator g) {
                g.emitSequence(this, subnodes);
            }
            
            @Override
            public String toString() {
                final StringBuilder b = new StringBuilder();
                String sep = "";
                for (final ValidatedPatternNode n : subnodes) {
                    b.append(sep).append(n);
                    sep = " ";
                }
                return "(" + b.toString() + ")";
            }
        };
    }
}
