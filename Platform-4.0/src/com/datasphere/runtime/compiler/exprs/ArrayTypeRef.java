package com.datasphere.runtime.compiler.exprs;

import com.datasphere.exception.*;
import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.compiler.visitors.*;

public class ArrayTypeRef extends ValueExpr
{
    public final TypeName typeName;
    
    public ArrayTypeRef(final TypeName typeName) {
        super(ExprCmd.ARRAYTYPE);
        this.typeName = typeName;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitArrayTypeRef(this, params);
    }
    
    @Override
    public Expr resolve() {
        throw new CompilationException("expected expression, got type" + this.typeName);
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.typeName;
    }
    
    @Override
    public Class<?> getType() {
        assert false;
        return null;
    }
    
    @Override
    public boolean isConst() {
        return true;
    }
}
