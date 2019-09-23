package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.visitors.*;
import java.util.*;

public class PredicateAsValueExpr extends ValueExpr
{
    private final Predicate p;
    
    public PredicateAsValueExpr(final Predicate p) {
        super(p.op);
        this.p = p;
    }
    
    @Override
    public Class<?> getType() {
        return this.p.getType();
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        Expr res = this.p.visit(visitor, params);
        if (res != null) {
            final PredicateAsValueExpr wrapper = new PredicateAsValueExpr((Predicate)res);
            res = wrapper.setOriginalExpr(res);
        }
        return res;
    }
    
    @Override
    public List<? extends Expr> getArgs() {
        return this.p.getArgs();
    }
    
    @Override
    public <T> void visitArgs(final ExpressionVisitor<T> v, final T params) {
        this.p.visitArgs(v, params);
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof PredicateAsValueExpr) {
            final PredicateAsValueExpr o = (PredicateAsValueExpr)other;
            return this.p.equals(o.p);
        }
        if (other instanceof Predicate) {
            final Predicate o2 = (Predicate)other;
            return this.p.equals(o2);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return this.p.hashCode();
    }
    
    @Override
    public String toString() {
        return this.p.toString();
    }
    
    @Override
    public boolean isConst() {
        return this.p.isConst();
    }
}
