package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.visitors.*;
import java.util.*;
import org.apache.commons.lang.builder.*;

public class ComparePredicate extends Predicate
{
    public final List<ValueExpr> args;
    
    public ComparePredicate(final ExprCmd op, final List<ValueExpr> args) {
        super(op);
        assert args != null;
        this.args = args;
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.args;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitComparePredicate(this, params);
    }
    
    @Override
    public List<? extends Expr> getArgs() {
        return this.args;
    }
    
    @Override
    public <T> void visitArgs(final ExpressionVisitor<T> v, final T params) {
        final ListIterator<ValueExpr> it = this.args.listIterator();
        while (it.hasNext()) {
            final ValueExpr e = it.next();
            final ValueExpr enew = (ValueExpr)v.visitExpr(e, params);
            if (enew != null) {
                it.set(enew);
            }
        }
    }
    
    @Override
    public boolean equals(final Object other) {
        return this == other || (other instanceof ComparePredicate && super.equals(other));
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(super.hashCode()).toHashCode();
    }
}
