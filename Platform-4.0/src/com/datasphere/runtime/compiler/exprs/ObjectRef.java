package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.select.*;
import com.datasphere.runtime.compiler.visitors.*;

public class ObjectRef extends ValueExpr
{
    public String name;
    private ValueExpr resolved;
    private ExprValidator ctx;
    
    public ObjectRef(final String name) {
        super(ExprCmd.VAR);
        this.name = name;
        this.resolved = null;
        this.ctx = null;
    }
    
    public void setContext(final ExprValidator ctx) {
        this.ctx = ctx;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        if (this.resolved != null) {
            return this.resolved.visit(visitor, params);
        }
        return visitor.visitObjectRef(this, params);
    }
    
    @Override
    public Expr resolve() {
        if (this.resolved == null && this.ctx != null) {
            this.resolved = this.ctx.findStaticVariableOrClass(this.name);
            if (this.resolved == null) {
                this.ctx.error("cannot resolve object " + this.name, this);
            }
        }
        return this.resolved;
    }
    
    @Override
    public Class<?> getType() {
        final Expr resolveExpr = this.resolve();
        if (resolveExpr != null) {
            return resolveExpr.getType();
        }
        return null;
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ObjectRef)) {
            return false;
        }
        final ObjectRef o = (ObjectRef)other;
        return super.equals(o) && this.name.equals(o.name);
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.name;
    }
}
