package com.datasphere.runtime.compiler.exprs;

public class CastToStringOperation extends CastOperation
{
    public CastToStringOperation(final ValueExpr arg) {
        super(arg, String.class);
    }
    
    @Override
    public String toString() {
        return "(" + this.getExpr() + ").toString()";
    }
}
