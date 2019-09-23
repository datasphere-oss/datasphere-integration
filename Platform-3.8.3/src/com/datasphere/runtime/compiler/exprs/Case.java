package com.datasphere.runtime.compiler.exprs;

public class Case
{
    public Expr cond;
    public ValueExpr expr;
    
    public Case(final Expr cond, final ValueExpr expr) {
        this.cond = cond;
        this.expr = expr;
    }
    
    @Override
    public String toString() {
        return "WHEN " + this.cond + " THEN " + this.expr;
    }
}
