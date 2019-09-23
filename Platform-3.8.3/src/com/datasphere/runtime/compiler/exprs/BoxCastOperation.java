package com.datasphere.runtime.compiler.exprs;

public class BoxCastOperation extends CastOperation
{
    public BoxCastOperation(final ValueExpr arg, final Class<?> targetType) {
        super(arg, targetType);
    }
    
    @Override
    public String toString() {
        final Class<?> t = this.getType();
        return t.getName() + ".valueOf(" + this.getExpr() + ")";
    }
}
