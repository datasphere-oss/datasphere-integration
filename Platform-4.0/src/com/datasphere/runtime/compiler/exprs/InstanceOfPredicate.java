package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.compiler.visitors.*;
import org.apache.commons.lang.builder.*;

public class InstanceOfPredicate extends ComparePredicate
{
    public final TypeName checktype;
    
    public InstanceOfPredicate(final ValueExpr expr, final TypeName type) {
        super(ExprCmd.INSTANCEOF, AST.NewList(expr));
        this.checktype = type;
    }
    
    public ValueExpr getExpr() {
        return this.args.get(0);
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.getExpr() + " instanceof " + this.checktype;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitInstanceOfPredicate(this, params);
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof InstanceOfPredicate)) {
            return false;
        }
        final InstanceOfPredicate o = (InstanceOfPredicate)other;
        return super.equals(other) && this.checktype.equals(o.checktype);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append((Object)this.checktype).append(super.hashCode()).toHashCode();
    }
}
