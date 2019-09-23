package com.datasphere.runtime.compiler.patternmatch;

import java.util.*;
import com.datasphere.runtime.utils.*;
import com.datasphere.runtime.compiler.*;

public abstract class PatternNode
{
    public List<PatternNode> getElementsIfType(final ComplexNodeType atype) {
        return null;
    }
    
    public abstract ValidatedPatternNode validate(final MatchValidator p0);
    
    @Override
    public abstract String toString();
    
    public static PatternNode makeRestartAnchor() {
        return new PatternNode() {
            @Override
            public ValidatedPatternNode validate(final MatchValidator v) {
                return v.visitRestartAnchor();
            }
            
            @Override
            public String toString() {
                return "#";
            }
        };
    }
    
    public static PatternNode makeRepetition(final PatternNode element, final PatternRepetition reps) {
        if (reps == null) {
            return element;
        }
        return new PatternNode() {
            @Override
            public ValidatedPatternNode validate(final MatchValidator v) {
                return v.visitRepetition(element, reps);
            }
            
            @Override
            public String toString() {
                return element + "" + reps;
            }
        };
    }
    
    public static PatternNode makeVariable(final String name) {
        return new PatternNode() {
            @Override
            public ValidatedPatternNode validate(final MatchValidator v) {
                return v.visitVariable(name);
            }
            
            @Override
            public String toString() {
                return name;
            }
        };
    }
    
    private static String getDelimiter(final ComplexNodeType type) {
        switch (type) {
            case ALTERNATION: {
                return "|";
            }
            case COMBINATION: {
                return "&";
            }
            case SEQUENCE: {
                return " ";
            }
            default: {
                return "";
            }
        }
    }
    
    private static PatternNode makeComplexNode(final ComplexNodeType type, final List<PatternNode> elements) {
        return new PatternNode() {
            @Override
            public ValidatedPatternNode validate(final MatchValidator v) {
                return v.visitComplexNode(type, elements);
            }
            
            @Override
            public List<PatternNode> getElementsIfType(final ComplexNodeType atype) {
                return (atype == type) ? elements : null;
            }
            
            @Override
            public String toString() {
                final String delim = getDelimiter(type);
                return "(" + StringUtils.join(elements, delim) + ")";
            }
        };
    }
    
    public static PatternNode makeComplexNode(final ComplexNodeType type, final PatternNode a, final PatternNode b) {
        final List<PatternNode> elements = a.getElementsIfType(type);
        if (elements != null) {
            return makeComplexNode(type, AST.NewList(elements, b));
        }
        return makeComplexNode(type, AST.NewList(a, b));
    }
}
