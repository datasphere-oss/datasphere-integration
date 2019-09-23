package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.patternmatch.*;
import com.datasphere.runtime.compiler.visitors.*;
import org.apache.commons.lang.builder.*;
import com.datasphere.runtime.compiler.select.*;

public class PatternVarRef extends DataSetRef
{
    private final PatternVariable var;
    
    public PatternVarRef(final PatternVariable var) {
        super(ExprCmd.PATTERN_VAR, -1);
        this.var = var;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitPatternVarRef(this, params);
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PatternVarRef)) {
            return false;
        }
        final PatternVarRef o = (PatternVarRef)other;
        return super.equals(o) && this.var.equals(o.var);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(super.hashCode()).append((Object)this.var.toString()).toHashCode();
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.var.getName();
    }
    
    @Override
    public Class<?> getType() {
        return this.var.getJavaType();
    }
    
    public int getVarId() {
        return this.var.getId();
    }
    
    @Override
    public DataSet getDataSet() {
        return this.var.getDataSet();
    }
}
