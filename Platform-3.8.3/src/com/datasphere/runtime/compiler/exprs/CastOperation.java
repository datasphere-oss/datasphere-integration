package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.compiler.visitors.*;

public class CastOperation extends Operation
{
    private final Class<?> targetType;
    
    public CastOperation(final ValueExpr arg, final Class<?> targetType) {
        super(ExprCmd.CAST, AST.NewList(arg));
        this.targetType = targetType;
        assert targetType != null;
    }
    
    @Override
    public String toString() {
        return this.exprToString() + " ((" + this.getType() + ")" + this.getExpr() + ")";
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitCastOperation(this, params);
    }
    
    public ValueExpr getExpr() {
        return this.args.get(0);
    }
    
    @Override
    public Class<?> getType() {
        return this.targetType;
    }
}
