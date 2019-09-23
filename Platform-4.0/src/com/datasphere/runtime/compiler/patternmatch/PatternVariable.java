package com.datasphere.runtime.compiler.patternmatch;

import com.datasphere.runtime.compiler.select.*;
import com.datasphere.runtime.compiler.exprs.*;

public abstract class PatternVariable
{
    private final String name;
    private final int id;
    
    PatternVariable(final int id, final String name) {
        this.id = id;
        this.name = name;
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PatternVariable)) {
            return false;
        }
        final PatternVariable o = (PatternVariable)other;
        return this.id == o.id;
    }
    
    @Override
    public int hashCode() {
        return this.id;
    }
    
    public int getId() {
        return this.id;
    }
    
    public String getName() {
        return this.name;
    }
    
    public boolean isTimer() {
        return false;
    }
    
    public Class<?> getJavaType() {
        return Object.class;
    }
    
    public void analyze(final MatchValidator v) {
    }
    
    public DataSet getDataSet() {
        return null;
    }
    
    public abstract void validate(final MatchValidator p0);
    
    public abstract void emit(final MatchExprGenerator p0);
    
    public abstract boolean isEventVar();
    
    @Override
    public abstract String toString();
    
    public static PatternVariable makeCondition(final int id, final String name, final Predicate pred) {
        return new PatternVariable(id, name) {
            Predicate p;
            
            @Override
            public void validate(final MatchValidator v) {
                this.p = v.validateCondtion(pred);
            }
            
            @Override
            public void emit(final MatchExprGenerator g) {
                g.emitCondition(id, this.p);
            }
            
            @Override
            public void analyze(final MatchValidator v) {
                this.p = v.analyzePredicate(this.p);
            }
            
            @Override
            public String toString() {
                return "(" + pred + ")";
            }
            
            @Override
            public boolean isEventVar() {
                return false;
            }
        };
    }
    
    public static PatternVariable makeVar(final int id, final String name, final Predicate pred, final String sname, final DataSet ds) {
        return new PatternVariable(id, name) {
            Predicate p;
            
            @Override
            public void validate(final MatchValidator v) {
                this.p = v.validateVarPredicate(pred, ds);
            }
            
            @Override
            public void emit(final MatchExprGenerator g) {
                g.emitVariable(id, this.p, ds);
            }
            
            @Override
            public void analyze(final MatchValidator v) {
                this.p = v.analyzePredicate(this.p);
            }
            
            @Override
            public Class<?> getJavaType() {
                return ds.getJavaType();
            }
            
            @Override
            public String toString() {
                return name + "(" + pred + ")";
            }
            
            @Override
            public boolean isEventVar() {
                return true;
            }
            
            @Override
            public DataSet getDataSet() {
                return ds;
            }
        };
    }
    
    public static PatternVariable makeTimerFunc(final int id, final String name, final Predicate pred, final String sname, final TimerFunc f) {
        switch (f) {
            case CREATE: {
                return new PatternVariable(id, name) {
                    long i;
                    
                    @Override
                    public void validate(final MatchValidator v) {
                        this.i = v.validateCreateTimer(pred);
                    }
                    
                    @Override
                    public void emit(final MatchExprGenerator g) {
                        g.emitCreateTimer(id, this.i);
                    }
                    
                    @Override
                    public boolean isTimer() {
                        return true;
                    }
                    
                    @Override
                    public String toString() {
                        return name + "->timer[" + id + "](" + this.i + ")";
                    }
                    
                    @Override
                    public boolean isEventVar() {
                        return false;
                    }
                };
            }
            case STOP: {
                return new PatternVariable(id, name) {
                    int timerid;
                    
                    @Override
                    public void validate(final MatchValidator v) {
                        this.timerid = v.validateStopTimer(pred);
                    }
                    
                    @Override
                    public void emit(final MatchExprGenerator g) {
                        g.emitStopTimer(id, this.timerid);
                    }
                    
                    @Override
                    public String toString() {
                        return "stop(" + this.timerid + ")";
                    }
                    
                    @Override
                    public boolean isEventVar() {
                        return false;
                    }
                };
            }
            case WAIT: {
                return new PatternVariable(id, name) {
                    int timerid;
                    
                    @Override
                    public void validate(final MatchValidator v) {
                        this.timerid = v.validateWaitTimer(pred);
                    }
                    
                    @Override
                    public void emit(final MatchExprGenerator g) {
                        g.emitWaitTimer(id, this.timerid);
                    }
                    
                    @Override
                    public String toString() {
                        return "wait(" + this.timerid + ")";
                    }
                    
                    @Override
                    public boolean isEventVar() {
                        return true;
                    }
                };
            }
            default: {
                throw new IllegalArgumentException();
            }
        }
    }
}
