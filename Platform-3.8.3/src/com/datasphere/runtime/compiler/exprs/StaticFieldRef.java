package com.datasphere.runtime.compiler.exprs;

import java.lang.reflect.*;
import com.datasphere.runtime.compiler.visitors.*;

public class StaticFieldRef extends ValueExpr
{
    public final Field fieldRef;
    
    public StaticFieldRef(final Field f) {
        super(ExprCmd.STATIC_FIELD);
        this.fieldRef = f;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitStaticFieldRef(this, params);
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StaticFieldRef)) {
            return false;
        }
        final StaticFieldRef o = (StaticFieldRef)other;
        return super.equals(o) && this.fieldRef.equals(o.fieldRef);
    }
    
    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.fieldRef;
    }
    
    @Override
    public Class<?> getType() {
        return this.fieldRef.getType();
    }
}
