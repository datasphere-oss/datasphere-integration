package com.datasphere.runtime.compiler.exprs;

import com.datasphere.exception.*;
import com.datasphere.runtime.compiler.visitors.*;

public class TypeRef extends ValueExpr
{
    public final Class<?> klass;
    
    public TypeRef(final Class<?> klass) {
        super(ExprCmd.TYPE);
        this.klass = klass;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitTypeRef(this, params);
    }
    
    @Override
    public boolean isTypeRef() {
        return true;
    }
    
    @Override
    public Expr resolve() {
        throw new CompilationException("expected expression, got type " + this.getType().getName());
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.getType().getName();
    }
    
    @Override
    public Class<?> getType() {
        return this.klass;
    }
    
    @Override
    public boolean isConst() {
        return true;
    }
}
