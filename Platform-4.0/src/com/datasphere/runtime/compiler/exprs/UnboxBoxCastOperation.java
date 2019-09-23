package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.*;

public class UnboxBoxCastOperation extends CastOperation
{
    public UnboxBoxCastOperation(final ValueExpr arg, final Class<?> targetType) {
        super(arg, targetType);
    }
    
    @Override
    public String toString() {
        final String targetType = this.getType().getCanonicalName();
        final String unboxedSourceType = CompilerUtils.getUnboxingType(this.getExpr().getType()).getCanonicalName();
        final String unboxedTargetType = CompilerUtils.getUnboxingType(this.getType()).getCanonicalName();
        return targetType + ".valueOf((" + unboxedTargetType + ")(" + this.getExpr() + ")." + unboxedSourceType + "Value())";
    }
}
