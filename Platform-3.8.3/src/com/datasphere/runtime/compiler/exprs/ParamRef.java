package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.compiler.visitors.*;
import org.apache.commons.lang.builder.*;

public class ParamRef extends ValueExpr
{
    private final String paramName;
    private int index;
    private Class<?> expectedType;
    
    public ParamRef(final String pname) {
        super(ExprCmd.PARAMETER);
        this.paramName = pname;
        this.index = -1;
    }
    
    @Override
    public Class<?> getType() {
        return CompilerUtils.ParamTypeUndefined.class;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitParamRef(this, params);
    }
    
    @Override
    public boolean isConst() {
        return false;
    }
    
    @Override
    public String toString() {
        return this.exprToString() + ":" + this.paramName;
    }
    
    public int getIndex() {
        return this.index;
    }
    
    public void setIndex(final int idx) {
        this.index = idx;
    }
    
    public String getName() {
        return this.paramName;
    }
    
    public void setExpectedType(final Class<?> type) {
        if (CompilerUtils.isParam(type)) {
            throw new RuntimeException("Paramter <" + this.paramName + "> has no type inferred");
        }
        if (this.expectedType == null) {
            this.expectedType = type;
        }
        else {
            if (!this.expectedType.isAssignableFrom(type)) {
                throw new RuntimeException("Paramter <" + this.paramName + "> is referred twice with incompatible types <" + type.getCanonicalName() + "> and <" + this.expectedType.getCanonicalName() + ">");
            }
            this.expectedType = type;
        }
    }
    
    public Class<?> getExpectedType() {
        return this.expectedType;
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ParamRef)) {
            return false;
        }
        final ParamRef o = (ParamRef)other;
        return super.equals(o) && this.index == o.index;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(super.hashCode()).append(this.index).toHashCode();
    }
}
