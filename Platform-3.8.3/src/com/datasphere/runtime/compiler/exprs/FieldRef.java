package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.compiler.visitors.*;
import com.datasphere.runtime.utils.*;
import org.apache.commons.lang.builder.*;

public abstract class FieldRef extends Operation
{
    private final String name;
    
    public static FieldRef createUnresolvedFieldRef(final ValueExpr expr, final String name) {
        return new FieldRef(expr, name) {
            @Override
            public Class<?> getType() {
                return Object.class;
            }
            
            @Override
            public String genFieldAccess(final String obj) {
                throw new RuntimeException("internal error: access unresolved field");
            }
        };
    }
    
    protected FieldRef(final ValueExpr expr, final String name) {
        super(ExprCmd.FIELD, AST.NewList(expr));
        this.name = name;
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.name;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitFieldRef(this, params);
    }
    
    public ValueExpr getExpr() {
        return this.args.get(0);
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FieldRef)) {
            return false;
        }
        final FieldRef o = (FieldRef)other;
        return super.equals(o) && NamePolicy.isEqual(this.name, o.name);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.name).append(super.hashCode()).toHashCode();
    }
    
    @Override
    public abstract Class<?> getType();
    
    public abstract String genFieldAccess(final String p0);
    
    public boolean isStatic() {
        return false;
    }
    
    public String getName() {
        return this.name;
    }
}
