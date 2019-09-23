package com.datasphere.runtime.compiler.exprs;

import java.util.*;
import com.datasphere.runtime.compiler.visitors.*;

public class StringConcatenation extends Operation
{
    public StringConcatenation(final List<ValueExpr> args) {
        super(ExprCmd.CONCAT, args);
    }
    
    @Override
    public Class<?> getType() {
        return String.class;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitStringConcatenation(this, params);
    }
}
