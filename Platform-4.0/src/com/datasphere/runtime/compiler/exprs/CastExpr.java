package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.compiler.visitors.*;
import org.apache.commons.lang.builder.*;

public class CastExpr extends Operation
{
    public final TypeName casttype;
    
    public CastExpr(final TypeName type, final ValueExpr arg) {
        super(ExprCmd.CAST, AST.NewList(arg));
        this.casttype = type;
    }
    
    @Override
    public String toString() {
        return this.exprToString() + "CAST(" + this.getExpr() + " AS " + this.casttype + ")";
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitCastExpr(this, params);
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CastExpr)) {
            return false;
        }
        final CastExpr o = (CastExpr)other;
        return super.equals(o) && this.casttype.equals(o.casttype);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)super.toString()).append((Object)this.casttype).toHashCode();
    }
    
    @Override
    public Class<?> getType() {
        return UndefinedType.class;
    }
    
    public ValueExpr getExpr() {
        return this.args.get(0);
    }
    
    public static class UndefinedType
    {
    }
}
