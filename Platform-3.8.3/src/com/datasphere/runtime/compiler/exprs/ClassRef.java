package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.compiler.visitors.*;

public class ClassRef extends ValueExpr
{
    public final TypeName typeName;
    
    public ClassRef(final TypeName typeName) {
        super(ExprCmd.CLASS);
        this.typeName = typeName;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitClassRef(this, params);
    }
    
    @Override
    public Class<?> getType() {
        return Class.class;
    }
    
    @Override
    public boolean isConst() {
        return true;
    }
}
