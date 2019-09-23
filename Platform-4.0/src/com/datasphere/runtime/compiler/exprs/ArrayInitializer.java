package com.datasphere.runtime.compiler.exprs;

import java.util.*;
import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.compiler.visitors.*;

public class ArrayInitializer extends Operation
{
    private final Class<?> arrayType;
    
    public ArrayInitializer(final Class<?> cls, final List<ValueExpr> arrayValues) {
        super(ExprCmd.ARRAYINIT, arrayValues);
        this.arrayType = CompilerUtils.toArrayType(cls);
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.arrayType + "[] {" + this.argsToString() + "}";
    }
    
    @Override
    public Class<?> getType() {
        return this.arrayType;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitArrayInitializer(this, params);
    }
}
