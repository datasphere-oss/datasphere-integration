package com.datasphere.runtime.compiler.exprs;

import com.datasphere.runtime.compiler.visitors.*;
import java.util.*;

public class CaseExpr extends ValueExpr
{
    private Class<?> resultType;
    private List<Expr> cachedargs;
    public ValueExpr selector;
    public final List<Case> cases;
    public ValueExpr else_expr;
    
    public CaseExpr(final ValueExpr selector, final List<Case> cases, final ValueExpr else_expr) {
        super(ExprCmd.CASE);
        this.cachedargs = null;
        this.selector = selector;
        this.cases = cases;
        this.else_expr = else_expr;
    }
    
    @Override
    public String toString() {
        String res = "";
        if (this.selector != null) {
            res += this.selector;
        }
        res += this.cases;
        if (this.else_expr != null) {
            res += this.else_expr;
        }
        return this.exprToString() + res + " END";
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitCase(this, params);
    }
    
    @Override
    public List<? extends Expr> getArgs() {
        if (this.cachedargs == null) {
            final List<Expr> args = new ArrayList<Expr>();
            if (this.selector != null) {
                args.add(this.selector);
            }
            for (final Case c : this.cases) {
                args.add(c.cond);
                args.add(c.expr);
            }
            if (this.else_expr != null) {
                args.add(this.else_expr);
            }
            this.cachedargs = args;
        }
        return this.cachedargs;
    }
    
    @Override
    public <T> void visitArgs(final ExpressionVisitor<T> v, final T params) {
        this.cachedargs = null;
        if (this.selector != null) {
            final ValueExpr enew = (ValueExpr)v.visitExpr(this.selector, params);
            if (enew != null) {
                this.selector = enew;
            }
        }
        for (final Case c : this.cases) {
            final Expr cnew = v.visitExpr(c.cond, params);
            if (cnew != null) {
                c.cond = cnew;
            }
            final ValueExpr enew2 = (ValueExpr)v.visitExpr(c.expr, params);
            if (enew2 != null) {
                c.expr = enew2;
            }
        }
        if (this.else_expr != null) {
            final ValueExpr enew = (ValueExpr)v.visitExpr(this.else_expr, params);
            if (enew != null) {
                this.else_expr = enew;
            }
        }
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
