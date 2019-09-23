package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.visitors.*;
import java.util.*;

public class LogicalPredicate extends Predicate
{
    public final List<Predicate> args;
    
    public LogicalPredicate(final ExprCmd op, final List<Predicate> args) {
        super(op);
        this.args = args;
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.args;
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitLogicalPredicate(this, params);
    }
    
    @Override
    public List<? extends Expr> getArgs() {
        return this.args;
    }
    
    @Override
    public <T> void visitArgs(final ExpressionVisitor<T> v, final T params) {
        final ListIterator<Predicate> it = this.args.listIterator();
        while (it.hasNext()) {
            final Predicate p = it.next();
            final Predicate pnew = (Predicate)v.visitExpr(p, params);
            if (pnew != null) {
                it.set(pnew);
            }
        }
    }
    
    @Override
    public boolean equals(final Object other) {
        return this == other || (other instanceof LogicalPredicate && super.equals(other));
    }
    
    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
