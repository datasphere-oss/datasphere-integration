package com.datasphere.runtime.compiler.exprs;

import java.util.*;
import com.datasphere.runtime.compiler.visitors.*;

public class NumericOperation extends Operation
{
    private Class<?> resultType;
    
    public NumericOperation(final ExprCmd op, final List<ValueExpr> args) {
        super(op, args);
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitNumericOperation(this, params);
    }
    
    @Override
    public Class<?> getType() {
        assert this.resultType != null;
        return this.resultType;
    }
    
    public void setType(final Class<?> resType) {
        assert resType != null;
        this.resultType = resType;
    }
}
