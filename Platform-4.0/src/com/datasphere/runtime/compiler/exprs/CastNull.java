package com.datasphere.runtime.compiler.exprs;

public class CastNull extends CastOperation
{
    public CastNull(final ValueExpr arg, final Class<?> targetType) {
        super(arg, targetType);
    }
    
    @Override
    public String toString() {
        final Class<?> t = this.getType();
        return "(" + t.getName() + ")null";
    }
}
