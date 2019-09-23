package com.datasphere.runtime.compiler.exprs;

public class UnboxCastOperation extends CastOperation
{
    public UnboxCastOperation(final ValueExpr arg, final Class<?> targetType) {
        super(arg, targetType);
    }
    
    @Override
    public String toString() {
        final Class<?> t = this.getType();
        return "(" + this.getExpr() + ")." + t.getSimpleName() + "Value()";
    }
}
