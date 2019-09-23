package com.datasphere.runtime.compiler.exprs;

public class CastParam extends CastOperation
{
    public CastParam(final ValueExpr arg, final Class<?> targetType) {
        super(arg, targetType);
    }
}
